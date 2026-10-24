//! In-process external CMD demo. No real storage shrink or durable execution.
//! cargo run --locked -p storage-pool --example shrink_commands
use async_trait::async_trait;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tokio::sync::{Semaphore, mpsc, oneshot};
use workflow_foundation::*;

// Domain capability: simulate moving one chunk, returning only after it is stable.
struct ShrinkService {
    completed: Mutex<u32>,
    first_started: Arc<Semaphore>,
    finish_first: Arc<Semaphore>,
}
#[async_trait]
impl ServiceHandler for ShrinkService {
    type Request = u32;
    type Reply = u32;
    type Query = ();
    type View = u32;
    fn resources(_: &u32) -> Vec<ResourceAccess> {
        vec![ResourceAccess::write(ObjectKey::new("demo-disk", "d1"))]
    }
    async fn execute(&self, chunk: u32, _: ActivityContext) -> Outcome<u32> {
        if chunk == 1 {
            self.first_started.add_permits(1);
            // Simulate an already-issued IO that must finish before suspension.
            self.finish_first
                .acquire()
                .await
                .map_err(|_| Failure::Unavailable)?
                .forget();
        }
        *self.completed.lock().unwrap() = chunk;
        println!("DOMAIN: chunk {chunk} committed");
        Ok(chunk)
    }
    fn query(&self, _: ()) -> Outcome<u32> {
        Ok(*self.completed.lock().unwrap())
    }
}

struct ShrinkWorkflow {
    service: ServiceClient<ShrinkService>,
    chunks: u32,
}
#[async_trait]
impl Workflow for ShrinkWorkflow {
    type Output = u32;
    const NAME: &'static str = "demo.shrink";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![ObjectKey::new("demo-disk", "d1")]
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<u32> {
        let mut completed = 0;
        for chunk in 1..=self.chunks {
            ctx.report_progress(
                "move-chunks",
                u64::from(completed),
                Some(u64::from(self.chunks)),
            );
            // activity() checks pause/cancel before dispatch. No other Run here.
            completed = ctx.activity(&self.service, chunk).await?;
        }
        ctx.report_progress(
            "shrink-complete",
            u64::from(completed),
            Some(u64::from(self.chunks)),
        );
        Ok(completed)
    }
}

// These messages represent separate external requests, potentially from different
// connections. Only the application coordinator retains Run/RunControl handles.
enum Command {
    Start {
        chunks: u32,
        reply: oneshot::Sender<Outcome<TaskId>>,
    },
    Suspend {
        id: TaskId,
        reply: oneshot::Sender<Outcome<()>>,
    },
    WaitSuspended {
        id: TaskId,
        reply: oneshot::Sender<Outcome<()>>,
    },
    Resume {
        id: TaskId,
        reply: oneshot::Sender<Outcome<()>>,
    },
    Finish {
        id: TaskId,
        reply: oneshot::Sender<Outcome<u32>>,
    },
}
fn unknown() -> Failure {
    Failure::Rejected("unknown or consumed operation id".into())
}
async fn commands(
    mut pool: WorkflowRuntime,
    service: ServiceClient<ShrinkService>,
    mut rx: mpsc::Receiver<Command>,
) -> Outcome<()> {
    let mut results: BTreeMap<TaskId, Run<u32>> = BTreeMap::new();
    let coordinator = pool.client();
    let mut waiters = tokio::task::JoinSet::new();
    while let Some(command) = rx.recv().await {
        match command {
            Command::Start { chunks, reply } => {
                let result = if chunks == 0 {
                    Err(Failure::Rejected("chunks must be positive".into()))
                } else {
                    pool.submit(ShrinkWorkflow {
                        service: service.clone(),
                        chunks,
                    })
                    .await
                    .map(|run| {
                        let id = run.id();
                        results.insert(id, run);
                        id
                    })
                };
                let _ = reply.send(result);
            }
            Command::Suspend { id, reply } => {
                // Acknowledges the REQUEST only, not that the activity has stopped.
                let _ = reply.send(coordinator.suspend(id));
            }
            Command::Resume { id, reply } => {
                let _ = reply.send(coordinator.resume(id));
            }
            Command::WaitSuspended { id, reply } => {
                let coordinator = coordinator.clone();
                waiters.spawn(async move {
                    let _ = reply.send(coordinator.wait_suspended(id).await);
                });
            }
            Command::Finish { id, reply } => match results.remove(&id) {
                Some(mut run) => {
                    waiters.spawn(async move {
                        let _ = reply.send(run.result().await);
                    });
                }
                None => {
                    let _ = reply.send(Err(unknown()));
                }
            },
        }
        while let Some(joined) = waiters.try_join_next() {
            joined.map_err(|_| Failure::Uncertain("command waiter panicked".into()))?;
        }
    }
    pool.shutdown().await?;
    while let Some(joined) = waiters.join_next().await {
        joined.map_err(|_| Failure::Uncertain("command waiter panicked".into()))?;
    }
    Ok(())
}
async fn request<T>(
    tx: &mpsc::Sender<Command>,
    make: impl FnOnce(oneshot::Sender<Outcome<T>>) -> Command,
) -> Outcome<T> {
    let (reply, response) = oneshot::channel();
    tx.send(make(reply))
        .await
        .map_err(|_| Failure::Unavailable)?;
    response.await.map_err(|_| Failure::Unavailable)?
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Outcome<()> {
    let mut pool = WorkflowRuntime::start("demo-pool", Config::default())?;
    let started = Arc::new(Semaphore::new(0));
    let finish_first = Arc::new(Semaphore::new(0));
    let service = pool.register(
        "shrink",
        ShrinkService {
            completed: Mutex::new(0),
            first_started: started.clone(),
            finish_first: finish_first.clone(),
        },
    )?;
    let (tx, rx) = mpsc::channel(16);
    let host = tokio::spawn(commands(pool, service.clone(), rx));

    let id = request(&tx, |reply| Command::Start { chunks: 3, reply }).await?;
    println!("CMD start: {id:?}");
    started
        .acquire()
        .await
        .map_err(|_| Failure::Unavailable)?
        .forget();
    request(&tx, |reply| Command::Suspend { id, reply }).await?;
    println!("CMD suspend {id:?}: accepted; first IO is still in flight");
    assert_eq!(service.query(()).await?, 0);

    finish_first.add_permits(1);
    request(&tx, |reply| Command::WaitSuspended { id, reply }).await?;
    let paused_progress = service.query(()).await?;
    assert_eq!(paused_progress, 1);
    println!("SUSPENDED {id:?}: completed {paused_progress}/3; chunk 2 has not started");

    request(&tx, |reply| Command::Resume { id, reply }).await?;
    println!("CMD resume {id:?}: same execution continues");
    let completed = request(&tx, |reply| Command::Finish { id, reply }).await?;
    assert_eq!(completed, 3);
    println!("COMPLETED {id:?}: {completed}/3; chunk 1 was not repeated");

    drop(tx);
    host.await
        .map_err(|_| Failure::Uncertain("command host terminated".into()))?
}
