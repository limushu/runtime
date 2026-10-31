//! cargo run -p workflow-foundation --example activity_trait [-- --cancel]
//! Demonstrates one shrink migration step with a simulated C balance service.
//! Both member disks are submitted together; no disks are actually removed.
use async_trait::async_trait;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::mpsc;
use workflow_foundation::*;

const DISKS: [&str; 2] = ["disk-a", "disk-b"];
const TOTAL: u64 = 8;

// Read the Workflow first: it only waits for the final business result.
struct ShrinkMigration(ServiceClient<Balance>);
#[async_trait]
impl Workflow for ShrinkMigration {
    type Output = ();
    const NAME: &'static str = "demo.shrink-migration";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        let moved = ctx
            .run_activity(MigrateDisks {
                balance: self.0,
                cursor: 0,
            })
            .await?;
        println!("[WF] 迁移完成：{moved}/{TOTAL}，现在才进入下一步");
        Ok(())
    }
}

// The Activity declares business readiness and retry policy, and interprets replies.
struct MigrateDisks {
    balance: ServiceClient<Balance>,
    cursor: u64,
}
#[async_trait]
impl Activity for MigrateDisks {
    type Reply = MigrationReply;
    type Output = u64;
    fn name(&self) -> &'static str {
        "shrink.migrate-disks"
    }
    fn description(&self) -> Option<&str> {
        Some("迁移待移除磁盘上的数据")
    }
    fn retry_policy(&self) -> RetryPolicy {
        RetryPolicy::fixed(2, Duration::from_millis(100))
    }
    fn run_condition(&self) -> Option<Arc<dyn RunCondition>> {
        Some(Arc::new(BalanceReadiness(self.balance.clone())))
    }
    async fn execute(&mut self, scope: &mut ActivityScope<'_>) -> Outcome<Self::Reply> {
        scope.call(&self.balance, self.cursor).await
    }
    fn handle(&mut self, result: Outcome<Self::Reply>) -> Outcome<ActivityAction<u64>> {
        Ok(match result? {
            MigrationReply::Busy => {
                println!("[Activity] 均衡暂忙，交给框架按策略重试");
                ActivityAction::Retry {
                    reason: "balance busy".into(),
                }
            }
            MigrationReply::Stopped(cursor) => {
                self.cursor = cursor;
                println!("[Activity] 保留游标={cursor}，等待恢复");
                ActivityAction::Suspend(ActivityWait::Resume)
            }
            MigrationReply::Completed(count) => ActivityAction::Continue(count),
        })
    }
}

// This checks the domain's readiness, not whether another Task is running.
// Cross-task exclusion belongs to TaskCenter's SchedulingPolicy.
struct BalanceReadiness(ServiceClient<Balance>);
#[async_trait]
impl RunCondition for BalanceReadiness {
    async fn check(&self) -> Outcome<Readiness> {
        self.0.query(()).await
    }
}

// Mock domain adapter: observe control, call the separate native stop API,
// await its acknowledgement, and only then return Stopped or Cancelled.
enum MigrationReply {
    Busy,
    Stopped(u64),
    Completed(u64),
}
struct Balance {
    ready: Arc<AtomicBool>,
    busy_once: AtomicBool,
    progress: mpsc::UnboundedSender<u64>,
}
#[async_trait]
impl ServiceHandler for Balance {
    type Request = u64;
    type Reply = MigrationReply;
    type Query = ();
    type View = Readiness;
    fn resources(_: &u64) -> Vec<ResourceAccess> {
        DISKS
            .iter()
            .map(|disk| ResourceAccess::write(ObjectKey::new("disk", *disk)))
            .collect()
    }
    async fn execute(&self, mut cursor: u64, ctx: ActivityContext) -> Outcome<MigrationReply> {
        if self.busy_once.swap(false, Ordering::SeqCst) {
            return Ok(MigrationReply::Busy);
        }
        println!("[Balance] 提交整组磁盘 {DISKS:?}，从游标={cursor} 开始");
        let mut control = ctx.control();
        while cursor < TOTAL {
            if control.is_cancelled() || control.request().paused() {
                println!("[Balance] 调用 C 停止接口，等待停稳……");
                tokio::time::sleep(Duration::from_millis(50)).await; // Simulated stop ACK.
                println!("[Balance] C 已停稳，游标={cursor}");
                return if control.is_cancelled() {
                    Err(Failure::Cancelled)
                } else {
                    Ok(MigrationReply::Stopped(cursor))
                };
            }
            tokio::select! {
                // Cancellation also wakes this branch; the loop must still stop C.
                _ = control.changed() => {},
                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                    cursor += 1; // Simulated native progress callback.
                    ctx.report_progress("shrink.migrate-disks", cursor, Some(TOTAL));
                    println!("[Balance] 进度 {cursor}/{TOTAL}");
                    let _ = self.progress.send(cursor);
                }
            }
        }
        Ok(MigrationReply::Completed(cursor))
    }
    fn query(&self, _: ()) -> Outcome<Readiness> {
        Ok(if self.ready.load(Ordering::SeqCst) {
            Readiness::Ready
        } else {
            Readiness::Blocked("balance not ready".into())
        })
    }
}

async fn demo() -> Outcome<()> {
    let cancel = std::env::args().any(|arg| arg == "--cancel");
    let mut runtime = WorkflowRuntime::start("demo-pool", Config::default())?;
    let (progress, mut updates) = mpsc::unbounded_channel();
    let ready = Arc::new(AtomicBool::new(true));
    let balance = runtime.register(
        "balance",
        Balance {
            ready: ready.clone(),
            busy_once: AtomicBool::new(true),
            progress,
        },
    )?;
    let mut task = runtime.submit(ShrinkMigration(balance)).await?;
    println!("[Task] {} 已提交", task.id());
    loop {
        let completed = updates.recv().await.ok_or(Failure::Unavailable)?;
        if completed >= 2 {
            break;
        }
    }
    if cancel {
        println!("[控制端] 请求取消");
        task.control.cancel();
    } else {
        println!("[控制端] 请求暂停");
        task.control.suspend()?;
        task.control.wait_suspended().await?;
        println!("[Task] {} 已确认暂停，WF 仍停在同一个 await", task.id());
        for step in task
            .control
            .activities()
            .entries
            .iter()
            .filter(|entry| entry.kind == ActivityKind::Step)
        {
            println!(
                "[逻辑步骤] #{} {:?}, attempts={}, retries={}, progress={}/{}",
                step.id.sequence,
                step.state,
                step.attempts,
                step.retries,
                step.progress.completed,
                TOTAL
            );
        }
        ready.store(false, Ordering::SeqCst);
        println!("[控制端] 请求恢复，但均衡暂未就绪");
        task.control.resume()?;
        loop {
            match task.control.state() {
                RunState::WaitingConditions(reason) => {
                    println!("[Activity] 框架自动检查条件并等待：{reason}");
                    break;
                }
                RunState::Failed(error) => return Err(error),
                _ => {
                    task.control.changed().await?;
                }
            }
        }
        println!("[领域事件] 均衡已就绪，通知框架重新检查");
        ready.store(true, Ordering::SeqCst);
        task.control.recheck();
    }
    match task.result().await {
        Err(Failure::Cancelled) if cancel => println!("[Task] 已取消，WF 不会进入下一步"),
        result => result?,
    }
    println!("[Task] 最终状态：{:?}", task.control.state());
    for call in task.control.activities().entries {
        println!(
            "[{:?}] #{} {} {:?}, parent={:?}, attempts={}, retries={}",
            call.kind,
            call.id.sequence,
            call.name,
            call.state,
            call.parent_id.map(|id| id.sequence),
            call.attempts,
            call.retries
        );
    }
    runtime.shutdown().await
}

#[tokio::main]
async fn main() -> Outcome<()> {
    tokio::time::timeout(Duration::from_secs(5), demo())
        .await
        .map_err(|_| Failure::Failed("demo 超时".into()))?
}
