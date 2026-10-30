//! The opt-in path: execute returns a domain reply, handle decides what to do,
//! and Workflow awaits the final value without a suspension loop.
use async_trait::async_trait;
use std::time::Duration;
use workflow_foundation::*;

struct Balance;
struct MoveDisks {
    disks: Vec<String>,
    cursor: u64,
}
enum BalanceReply {
    Stopped { cursor: u64 },
    Completed { moved: u64 },
}
#[async_trait]
impl ServiceHandler for Balance {
    type Request = MoveDisks;
    type Reply = BalanceReply;
    type Query = ();
    type View = ();
    fn resources(request: &MoveDisks) -> Vec<ResourceAccess> {
        request
            .disks
            .iter()
            .map(|disk| ResourceAccess::write(ObjectKey::new("disk", disk)))
            .collect()
    }
    async fn execute(&self, request: MoveDisks, ctx: ActivityContext) -> Outcome<BalanceReply> {
        if request.cursor != 0 {
            println!("domain resumes from cursor {}", request.cursor);
            return Ok(BalanceReply::Completed { moved: 2 });
        }
        println!("domain starts migrating {:?}", request.disks);
        ctx.report_progress("migrating", 1, Some(2));
        let mut control = ctx.control();
        while !control.is_cancelled() && !control.request().paused() {
            let _ = control.changed().await;
        }
        // Stand-in for sending C's stop command and awaiting its real ACK.
        println!("domain sends stop command, then awaits acknowledgement");
        tokio::time::sleep(Duration::from_millis(10)).await;
        if control.is_cancelled() {
            return Err(Failure::Cancelled);
        }
        Ok(BalanceReply::Stopped { cursor: 1 })
    }
    fn query(&self, _: ()) -> Outcome<()> {
        Ok(())
    }
}

struct MigrateDisks {
    balance: ServiceClient<Balance>,
    disks: Vec<String>,
    cursor: u64,
}
#[async_trait]
impl ManagedActivity for MigrateDisks {
    type Reply = BalanceReply;
    type Output = u64;
    fn name(&self) -> &'static str {
        "迁移目标盘的数据"
    }
    fn description(&self) -> Option<&str> {
        Some("停稳后保存续作位置，等待恢复")
    }
    fn resources(&self) -> Vec<ResourceAccess> {
        self.disks
            .iter()
            .map(|disk| ResourceAccess::write(ObjectKey::new("disk", disk)))
            .collect()
    }
    async fn execute(&mut self, scope: &mut ActivityScope) -> Outcome<BalanceReply> {
        scope
            .call(
                &self.balance,
                MoveDisks {
                    disks: self.disks.clone(),
                    cursor: self.cursor,
                },
            )
            .await
    }
    fn handle(&mut self, result: Outcome<BalanceReply>) -> Outcome<ActivityAction<u64>> {
        Ok(match result? {
            BalanceReply::Completed { moved } => ActivityAction::Continue(moved),
            BalanceReply::Stopped { cursor } => {
                self.cursor = cursor;
                ActivityAction::Suspend(ActivityWait::Runnable)
            }
        })
    }
}

struct MigrationWorkflow(ServiceClient<Balance>);
#[async_trait]
impl Workflow for MigrationWorkflow {
    type Output = u64;
    const NAME: &'static str = "example.managed-migration";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<u64> {
        let moved = ctx
            .run_managed_activity(MigrateDisks {
                balance: self.0,
                disks: vec!["disk-a".into(), "disk-b".into()],
                cursor: 0,
            })
            .await?;
        println!("WF continues to its next step only after migration completes");
        Ok(moved)
    }
}

#[tokio::main]
async fn main() -> Outcome<()> {
    let mut runtime = WorkflowRuntime::start("example", Config::default())?;
    let balance = runtime.register("balance", Balance)?;
    let mut run = runtime.submit(MigrationWorkflow(balance)).await?;
    let mut observer = runtime.client().observe_task(run.id())?;
    while observer.snapshot().progress.completed == 0 {
        observer.changed().await?;
    }
    run.control.suspend()?;
    run.control.wait_suspended().await?;
    println!(
        "Task {} suspended: WF still awaits; the resource lease is released",
        run.id()
    );
    run.control.resume()?;
    let moved = run.result().await?;
    println!("Task {} completed: {moved} units moved", run.id());
    runtime.shutdown().await
}
