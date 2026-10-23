//! TaskCenter history and opt-in continuation; simulated migration, no real disk IO.
//! cargo run -p storage-pool --features kv-redb --example task_persistence -- /tmp/tasks.redb
use async_trait::async_trait;
use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};
use storage_pool::{
    task_center::{TaskCenter, persistence::*},
    *,
};
use tokio::sync::Semaphore;
use workflow_foundation::{
    ActivityContext, ObjectKey, ResourceAccess, RunState, ServiceClient, ServiceHandler, Workflow,
    WorkflowContext, WorkflowDriver,
};

struct Migration {
    completed: Arc<AtomicU32>,
    started: Arc<Semaphore>,
    release: Arc<Semaphore>,
    gate_first: bool,
}
#[async_trait]
impl ServiceHandler for Migration {
    type Request = u32;
    type Reply = u32;
    type Query = ();
    type View = u32;
    fn activity_name(_: &u32) -> &'static str {
        "shrink.move-chunk"
    }
    fn resources(_: &u32) -> Vec<ResourceAccess> {
        vec![ResourceAccess::write(ObjectKey::new("demo-disk", "disk-7"))]
    }
    async fn execute(&self, chunk: u32, _: ActivityContext) -> Outcome<u32> {
        if self.gate_first && chunk == 1 {
            self.started.add_permits(1);
            self.release
                .acquire()
                .await
                .map_err(|_| Failure::Unavailable)?
                .forget();
        }
        let previous = self.completed.load(Ordering::SeqCst);
        if chunk != previous + 1 {
            return Err(Failure::Rejected("duplicate migration".into()));
        }
        self.completed.store(chunk, Ordering::SeqCst);
        println!("domain confirmed chunk {chunk}");
        Ok(chunk)
    }
    fn query(&self, _: ()) -> Outcome<u32> {
        Ok(self.completed.load(Ordering::SeqCst))
    }
}
struct Shrink {
    migration: ServiceClient<Migration>,
    completed: u32,
    total: u32,
}
#[async_trait]
impl Workflow for Shrink {
    type Output = u32;
    const NAME: &'static str = "demo.shrink";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![ObjectKey::new("demo-disk", "disk-7")]
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<u32> {
        for chunk in self.completed + 1..=self.total {
            let completed = ctx.activity(&self.migration, chunk).await?;
            ctx.save_checkpoint(serde_json::to_vec(&completed).map_err(codec)?)
                .await?;
            ctx.report_progress("migrating", completed.into(), Some(self.total.into()));
        }
        Ok(self.total)
    }
}
fn codec(e: impl std::fmt::Display) -> Failure {
    Failure::Failed(e.to_string())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Outcome<()> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/task-center-demo.redb".into());
    let store: Arc<dyn TaskStore> = Arc::new(
        tokio::task::spawn_blocking(move || RedbTaskStore::open(path))
            .await
            .map_err(codec)??,
    );
    let pool = PoolUuid::new();
    let config = Config::default();
    let completed = Arc::new(AtomicU32::new(0));
    let started = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let mut driver = WorkflowDriver::new("demo-pool", config.clone())?;
    let migration = driver.register(
        "migration",
        Migration {
            completed: completed.clone(),
            started: started.clone(),
            release: release.clone(),
            gate_first: true,
        },
    )?;
    let old = TaskCenter::with_task_store(
        driver,
        &config,
        pool,
        PoolInstanceId::new(),
        store.clone(),
        PersistenceConfig::default(),
    )?;
    old.recover_tasks().await?;
    let task = old
        .submit_resumable(
            Shrink {
                migration,
                completed: 0,
                total: 3,
            },
            ResumeSpec {
                kind: "demo.shrink".into(),
                version: 1,
                input: b"3".to_vec(),
                checkpoint: b"0".to_vec(),
            },
        )
        .await?;
    let id = task.id();
    started.acquire().await.map_err(codec)?.forget();
    old.suspend_task(id).await?;
    release.add_permits(1);
    old.wait_suspended(task.id()).await?;
    old.shutdown().await?;
    let checkpoint: u32 =
        serde_json::from_slice(&old.task_detail(id).await?.resume.unwrap().checkpoint)
            .map_err(codec)?;
    println!("handoff: task {id}, checkpoint {checkpoint}");

    // Same KV and logical Pool UUID, new host and NEW service clients.
    // completed models existing domain progress across a controller handoff, not real durable storage.
    let mut driver = WorkflowDriver::new("demo-pool", config.clone())?;
    let migration = driver.register(
        "migration",
        Migration {
            completed,
            started,
            release,
            gate_first: false,
        },
    )?;
    let new = TaskCenter::with_task_store(
        driver,
        &config,
        pool,
        PoolInstanceId::new(),
        store,
        PersistenceConfig::default(),
    )?;
    new.register_recovery("demo.shrink", 1, move |task| {
        let migration = migration.clone();
        async move {
            let resume = task
                .resume
                .ok_or_else(|| Failure::Rejected("missing resume data".into()))?;
            let total: u32 = serde_json::from_slice(&resume.input).map_err(codec)?;
            let saved: u32 = serde_json::from_slice(&resume.checkpoint).map_err(codec)?;
            let actual = migration.query(()).await?;
            if actual < saved || actual > total {
                return Err(Failure::Uncertain("migration needs verification".into()));
            }
            Ok(Shrink {
                migration,
                completed: actual,
                total,
            })
        }
    })?;
    let report = new.recover_tasks().await?;
    println!("recovery: {report:?}");
    new.wait_suspended(id).await?;
    new.flush_history().await?;
    assert_eq!(new.task_detail(id).await?.status, TaskStatus::Paused);
    new.resume_task(id).await?;
    let mut observer = new.observe_task(id)?;
    while !matches!(
        observer.snapshot().execution.as_ref().unwrap().state,
        RunState::Completed | RunState::Failed(_)
    ) {
        observer.changed().await?;
    }
    new.flush_history().await?;
    let stored = new.task_detail(id).await?;
    assert_eq!(stored.status, TaskStatus::Completed);
    println!(
        "task {}: {:?}, executions={}",
        stored.id,
        stored.status,
        stored.executions.len()
    );
    for execution in stored.executions {
        println!(
            "  execution {}: {:?}, activities={}",
            execution.attempt,
            execution.state,
            execution.activities.len()
        );
        for activity in execution.activities.values() {
            println!(
                "    #{} {} {:?}",
                activity.sequence, activity.name, activity.state
            );
        }
    }
    new.shutdown().await
}
