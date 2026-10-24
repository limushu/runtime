//! Framework acceptance demo, not physical disk evacuation. Both modes use ordinary
//! Activity calls and the same TaskCenter. Existing storage workflows are unchanged.
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use storage_pool::{task_center::*, *};
use tokio::sync::{Notify, watch};
use workflow_foundation::{persistence::*, *};

#[derive(Clone, Serialize, Deserialize)]
struct Input {
    disk: String,
    stop_returns: bool,
}
#[derive(Serialize)]
struct Details {
    disk: String,
    moved: u64,
    total: u64,
}
struct Conditions(AtomicBool);
#[async_trait]
impl ContinuationGuard for Conditions {
    async fn check(&self) -> Outcome<ContinueDecision> {
        Ok(if self.0.load(Ordering::SeqCst) {
            ContinueDecision::Ready
        } else {
            ContinueDecision::Wait("waiting for valid placement".into())
        })
    }
}

// A domain operation's short control commands are independent of its long wait.
// pause() confirms a safe chunk boundary; it does not mean the whole migration finished.
struct MigrationOperation {
    pause: AtomicBool,
    parked: watch::Sender<bool>,
    changed: Notify,
}
#[async_trait]
impl ActivityControl for MigrationOperation {
    async fn suspend(&self) -> Outcome<()> {
        let mut parked = self.parked.subscribe();
        self.pause.store(true, Ordering::SeqCst);
        self.changed.notify_one();
        while !*parked.borrow_and_update() {
            parked.changed().await.map_err(|_| Failure::Unavailable)?;
        }
        Ok(())
    }
    async fn resume(&self) -> Outcome<()> {
        self.pause.store(false, Ordering::SeqCst);
        self.changed.notify_one();
        Ok(())
    }
}
struct Migrate {
    cursor: u64,
    stop_returns: bool,
}
struct Migration;
#[async_trait]
impl ServiceHandler for Migration {
    type Request = Migrate;
    type Reply = u64;
    type Query = ();
    type View = ();
    fn resources(_: &Migrate) -> Vec<ResourceAccess> {
        vec![]
    }
    async fn execute(&self, request: Migrate, ctx: ActivityContext) -> Outcome<u64> {
        let operation = Arc::new(MigrationOperation {
            pause: AtomicBool::new(false),
            parked: watch::channel(false).0,
            changed: Notify::new(),
        });
        if !request.stop_returns {
            ctx.register_control(operation.clone())?;
        }
        let mut token = ctx.control();
        let mut cursor = request.cursor;
        while cursor < 10 {
            if request.stop_returns && token.request().paused() {
                return Ok(cursor);
            }
            if operation.pause.load(Ordering::SeqCst) {
                operation.parked.send_replace(true);
                tokio::select! { _ = operation.changed.notified() => {}, r = token.changed() => { r?; } }
                continue;
            }
            operation.parked.send_replace(false);
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(25)) => { cursor += 1; },
                _ = operation.changed.notified() => {},
                r = token.changed() => { r?; },
            }
        }
        // Completion racing a suspend is also safely settled.
        operation.parked.send_replace(true);
        Ok(cursor)
    }
    fn query(&self, _: ()) -> Outcome<()> {
        Ok(())
    }
}
#[derive(Clone)]
struct Services {
    migration: ServiceClient<Migration>,
    conditions: Arc<Conditions>,
}
struct Shrink {
    input: Input,
    services: Services,
}
#[async_trait]
impl Workflow for Shrink {
    type Output = ();
    const NAME: &'static str = "demo.shrink";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![ObjectKey::new("disk", self.input.disk.clone())]
    }
    fn persistence(&self) -> Outcome<Persistence> {
        Persistence::durable(1, &self.input)
    }
    fn restore(record: &SavedTask, dependencies: &Dependencies) -> Outcome<Self> {
        if record.version != 1 {
            return Err(Failure::Rejected("unsupported shrink input version".into()));
        }
        Ok(Self {
            input: serde_json::from_value(record.input.clone())
                .map_err(|e| Failure::Rejected(e.to_string()))?,
            services: (*dependencies.get::<Services>()?).clone(),
        })
    }
    fn continuation_guard(&self) -> Option<Arc<dyn ContinuationGuard>> {
        Some(self.services.conditions.clone())
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        let mut cursor = ctx.restored_checkpoint().as_u64().unwrap_or(0);
        while cursor < 10 {
            cursor = ctx
                .activity(
                    &self.services.migration,
                    Migrate {
                        cursor,
                        stop_returns: self.input.stop_returns,
                    },
                )
                .await?;
            ctx.report_details(&Details {
                disk: self.input.disk.clone(),
                moved: cursor,
                total: 10,
            })?;
            ctx.save_checkpoint(&cursor).await?;
            // Retained calls arrive here when complete. Stopping calls may return a
            // partial result; persist it BEFORE acknowledging the task's safe pause.
            ctx.safe_point().await?;
        }
        Ok(())
    }
}
impl PoolWorkflow for Shrink {
    fn operation(&self) -> PoolOperation {
        PoolOperation {
            kind: OperationKind::Shrink(DiskId::new(&self.input.disk).unwrap()),
            objects: self.objects(),
        }
    }
}
struct Topology;
#[async_trait]
impl Workflow for Topology {
    type Output = ();
    const NAME: &'static str = "demo.topology";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        ctx.sleep(Duration::from_millis(20)).await
    }
}
impl PoolWorkflow for Topology {
    fn operation(&self) -> PoolOperation {
        PoolOperation {
            kind: OperationKind::OnlineDisk(DiskId::new("other-disk").unwrap()),
            objects: vec![],
        }
    }
}
async fn example(stop_returns: bool, ids: TaskIds) -> Outcome<()> {
    let config = Config::default();
    // The application has acquired/fenced leadership before supplying this epoch.
    let mut driver = WorkflowDriver::new("pool", config.clone())?
        .with_task_ids(if stop_returns { 2 } else { 1 }, ids)?;
    let services = Services {
        migration: driver.register("migration", Migration)?,
        conditions: Arc::new(Conditions(AtomicBool::new(true))),
    };
    driver.define::<Shrink>(Dependencies::default().with(services.clone()))?;
    driver
        .enable_persistence(Arc::new(MemoryTaskStore::default()))
        .await?;
    let restored = driver.restore().await?;
    let center =
        TaskCenter::from_restored(driver, &config, Arc::new(PoolSchedulingPolicy), restored)?;
    let mut run = center
        .submit(Shrink {
            input: Input {
                disk: "disk-a".into(),
                stop_returns,
            },
            services: services.clone(),
        })
        .await?;
    tokio::time::sleep(Duration::from_millis(60)).await;
    services.conditions.0.store(false, Ordering::SeqCst);
    let mut topology = center.submit(Topology).await?;
    topology.result().await?;
    let mut control = run.control.clone();
    while !matches!(control.state(), RunState::WaitingConditions(_)) {
        control.changed().await?;
    }
    println!(
        "{} stop_returns={stop_returns}: {:?}",
        run.id(),
        center.task(run.id())?.state
    );
    services.conditions.0.store(true, Ordering::SeqCst);
    run.control.recheck();
    run.result().await?;
    let task = center.task(run.id())?;
    println!(
        "same task {}, activities={}, details={}",
        task.id, task.activities.total, task.details
    );
    center.shutdown().await
}
#[tokio::main]
async fn main() -> Outcome<()> {
    let ids = TaskIds::for_leader(42)?;
    tokio::time::timeout(Duration::from_secs(10), async {
        example(false, ids.clone()).await?;
        example(true, ids).await
    })
    .await
    .map_err(|_| Failure::Failed("demo timed out".into()))?
}
