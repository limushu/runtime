//! cargo run -p workflow-foundation --example noncancellable_activity
//! Simulated local commit: no real metadata is written.
use async_trait::async_trait;
use std::time::Duration;
use tokio::sync::oneshot;
use workflow_foundation::*;

struct Commit {
    entered: Option<oneshot::Sender<()>>,
}
#[async_trait]
impl Activity for Commit {
    type Reply = ();
    type Output = ();
    fn name(&self) -> &'static str {
        "commit"
    }
    fn cancellable(&self) -> bool {
        false
    }
    async fn execute(&mut self, scope: &mut ActivityScope<'_>) -> Outcome<()> {
        println!("[Commit] 已进入不可取消步骤");
        if let Some(entered) = self.entered.take() {
            entered.send(()).map_err(|_| Failure::Unavailable)?;
        }
        // Simulated local work. Pause/host shutdown still affect this wait.
        scope.sleep(Duration::from_millis(250)).await?;
        println!("[Commit] 已完成，返回真实结果");
        Ok(())
    }
    fn handle(&mut self, result: Outcome<()>) -> Outcome<ActivityAction<()>> {
        result.map(ActivityAction::Continue)
    }
}
struct Next;
#[async_trait]
impl Activity for Next {
    type Reply = ();
    type Output = ();
    fn name(&self) -> &'static str {
        "next-step"
    }
    async fn execute(&mut self, _: &mut ActivityScope<'_>) -> Outcome<()> {
        Err(Failure::Failed(
            "pending cancellation should prevent this step".into(),
        ))
    }
    fn handle(&mut self, result: Outcome<()>) -> Outcome<ActivityAction<()>> {
        result.map(ActivityAction::Continue)
    }
}
struct Flow(Commit);
#[async_trait]
impl Workflow for Flow {
    type Output = ();
    const NAME: &'static str = "demo.noncancellable";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        ctx.run_activity(self.0).await?;
        println!("[WF] 收到 Commit 的完成结果；下一步将响应之前的取消请求");
        ctx.run_activity(Next).await
    }
}
#[tokio::main]
async fn main() -> Outcome<()> {
    let mut runtime = WorkflowRuntime::start("demo", Config::default())?;
    let (entered, started) = oneshot::channel();
    let mut run = runtime
        .submit(Flow(Commit {
            entered: Some(entered),
        }))
        .await?;
    started.await.map_err(|_| Failure::Unavailable)?;
    println!("[控制端] 请求取消 Task，当前 Commit 继续执行");
    run.control.cancel();
    let result = run.result().await;
    for step in &run.control.activities().entries {
        println!(
            "[Activity] {} cancellable={} attempts={} {:?}",
            step.name, step.cancellable, step.attempts, step.state
        );
    }
    runtime.shutdown().await?;
    match result {
        Err(Failure::Cancelled) => {
            println!("[Task] Commit 完成；后续步骤未执行；Task 已取消");
            Ok(())
        }
        Err(error) => Err(error),
        Ok(()) => Err(Failure::Failed(
            "Task should observe pending cancellation".into(),
        )),
    }
}
