//! A named Activity owns its inputs and interprets each domain reply.
//! The framework owns retries; Workflow handles suspension and resumption.
use async_trait::async_trait;
use std::{
    sync::atomic::{AtomicU32, Ordering},
    time::Duration,
};
use workflow_foundation::*;

#[derive(Default)]
struct Probe(AtomicU32);
enum ProbeReply {
    Pending { remaining: u32 },
    Ready(u32),
}
#[async_trait]
impl ServiceHandler for Probe {
    type Request = Vec<String>;
    type Reply = ProbeReply;
    type Query = ();
    type View = ();
    fn resources(disks: &Vec<String>) -> Vec<ResourceAccess> {
        disks
            .iter()
            .map(|disk| ResourceAccess::read(ObjectKey::new("disk", disk)))
            .collect()
    }
    async fn execute(&self, _: Vec<String>, ctx: ActivityContext) -> Outcome<ProbeReply> {
        let attempt = self.0.fetch_add(1, Ordering::SeqCst) + 1;
        println!("  domain attempt={attempt}, activity={:?}", ctx.id);
        Ok(if attempt == 1 {
            ProbeReply::Pending { remaining: 1 }
        } else {
            ProbeReply::Ready(attempt)
        })
    }
    fn query(&self, _: ()) -> Outcome<()> {
        Ok(())
    }
}

// Inputs, metadata, one attempt and its business decision live in one place.
struct CheckDisks {
    client: ServiceClient<Probe>,
    disks: Vec<String>,
}
#[async_trait]
impl Activity for CheckDisks {
    type Output = u32;

    fn name(&self) -> &'static str {
        "检查目标盘是否就绪"
    }
    fn description(&self) -> Option<&str> {
        Some("检查目标盘的领域状态，暂未就绪时等待后重试")
    }
    fn resources(&self) -> Vec<ResourceAccess> {
        self.client.resources(&self.disks)
    }
    fn retry(&self) -> Option<Retry> {
        Some(Retry::interval(Duration::from_secs(60)).max_attempts(3))
    }
    async fn execute(&mut self, scope: &mut ActivityScope) -> Outcome<ActivityDecision<u32>> {
        match scope.call(&self.client, self.disks.clone()).await? {
            ProbeReply::Pending { remaining } => Ok(ActivityDecision::Retry {
                reason: format!("还有 {remaining} 个目标盘尚未就绪"),
            }),
            ProbeReply::Ready(attempts) => Ok(ActivityDecision::Complete(attempts)),
        }
    }
}

struct WaitForReady {
    client: ServiceClient<Probe>,
    disks: Vec<String>,
}
#[async_trait]
impl Workflow for WaitForReady {
    type Output = u32;
    const NAME: &'static str = "example.wait-for-ready";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<u32> {
        loop {
            let exit = ctx
                .run_activity(CheckDisks {
                    client: self.client.clone(),
                    disks: self.disks.clone(),
                })
                .await?;
            match exit {
                ActivityExit::Completed(attempts) => return Ok(attempts),
                // No business checkpoint is needed for this read-only probe.
                ActivityExit::Suspended(()) => ctx.wait_until_runnable().await?,
            }
        }
    }
}

#[tokio::main]
async fn main() -> Outcome<()> {
    let mut runtime = WorkflowRuntime::start("example", Config::default())?;
    let client = runtime.register("probe", Probe::default())?;
    let mut run = runtime
        .submit(WaitForReady {
            client,
            disks: vec!["disk-a".into(), "disk-b".into()],
        })
        .await?;
    let id = run.id();
    let mut observer = runtime.client().observe_task(id)?;
    while !observer
        .snapshot()
        .activities
        .entries
        .back()
        .is_some_and(|step| step.retry_at.is_some())
    {
        observer.changed().await?;
    }
    run.control.suspend()?;
    run.control.wait_suspended().await?;
    println!("Task {id} suspended between attempts");
    run.control.resume()?;
    let attempts = run.result().await?;
    println!(
        "Task {id} completed: {attempts} calls across {} Activity invocations",
        run.control.activities().total
    );
    runtime.shutdown().await
}
