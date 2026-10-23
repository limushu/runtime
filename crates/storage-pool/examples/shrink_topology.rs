//! Domain command/reply simulation, not real disk IO or metadata mutation.
//! cargo run --offline --locked -p storage-pool --example shrink_topology
use async_trait::async_trait;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use storage_pool::task_center::TaskCenter;
use tokio::sync::{mpsc, oneshot, watch};
use workflow_foundation::*;

enum Command {
    Start(oneshot::Sender<Outcome<()>>),
    Pause(oneshot::Sender<Outcome<()>>),
    Resume(oneshot::Sender<Outcome<()>>),
    Cancel(oneshot::Sender<Outcome<()>>),
}
#[derive(Clone)]
struct Balance {
    commands: mpsc::Sender<Command>,
    progress: watch::Receiver<u64>,
    controls: Arc<tokio::sync::Mutex<()>>,
}
impl Balance {
    async fn control(
        &self,
        command: impl FnOnce(oneshot::Sender<Outcome<()>>) -> Command,
    ) -> Outcome<()> {
        let _guard = self.controls.lock().await;
        self.send(command).await
    }
    async fn send(
        &self,
        make: impl FnOnce(oneshot::Sender<Outcome<()>>) -> Command,
    ) -> Outcome<()> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(make(reply))
            .await
            .map_err(|_| Failure::Unavailable)?;
        response
            .await
            .map_err(|_| Failure::Uncertain("balance reply lost".into()))?
    }
}
#[async_trait]
impl ActivityPause for Balance {
    async fn pause(&self) -> Outcome<()> {
        self.control(Command::Pause).await
    }
    async fn resume(&self) -> Outcome<()> {
        self.control(Command::Resume).await
    }
}
// Simulates the existing balancing domain service. It owns one whole-disk job J7.
fn balancing_service() -> (Balance, tokio::task::JoinHandle<()>) {
    let (commands, mut rx) = mpsc::channel(16);
    let (progress_tx, progress) = watch::channel(0);
    let host = tokio::spawn(async move {
        let mut finished: Option<oneshot::Sender<Outcome<()>>> = None;
        let mut paused = false;
        let mut done = 0;
        let mut timer = tokio::time::interval(Duration::from_millis(80));
        loop {
            tokio::select! {
                command = rx.recv() => match command {
                    None => break,
                    Some(Command::Start(reply)) => {
                        println!("BALANCE J7: start one whole-disk evacuation job");
                        finished = Some(reply);
                    }
                    Some(Command::Pause(reply)) => {
                        // A real service settles in-flight IO here BEFORE acknowledging.
                        paused = true;
                        println!("BALANCE J7: safely paused at {done}/5");
                        let _ = reply.send(Ok(()));
                    }
                    Some(Command::Resume(reply)) => {
                        paused = false;
                        println!("BALANCE J7: resume same job against current topology");
                        let _ = reply.send(Ok(()));
                    }
                    Some(Command::Cancel(reply)) => {
                        if let Some(finished) = finished.take() { let _ = finished.send(Err(Failure::Cancelled)); }
                        let _ = reply.send(Ok(()));
                    }
                },
                _ = timer.tick(), if finished.is_some() && !paused => {
                    done += 1;
                    progress_tx.send_replace(done);
                    if done == 5 {
                        println!("BALANCE J7: evacuation completed");
                        let _ = finished.take().unwrap().send(Ok(()));
                    }
                }
            }
        }
    });
    (
        Balance {
            commands,
            progress,
            controls: Arc::default(),
        },
        host,
    )
}

struct Conditions(AtomicBool);
#[async_trait]
impl ContinuationGuard for Conditions {
    async fn check(&self) -> Outcome<ContinueDecision> {
        Ok(if self.0.load(Ordering::SeqCst) {
            ContinueDecision::Ready
        } else {
            ContinueDecision::Wait("waiting for sufficient healthy capacity".into())
        })
    }
}
#[derive(Clone, Copy)]
enum Step {
    RequestShrink,
    DisableAllocation,
    Evacuate,
    VerifyEmpty,
    Down,
    Removed,
}
struct DomainSteps(Balance);
#[async_trait]
impl ServiceHandler for DomainSteps {
    type Request = Step;
    type Reply = ();
    type Query = ();
    type View = ();
    fn activity_name(step: &Step) -> &'static str {
        match step {
            Step::RequestShrink => "request-shrink",
            Step::DisableAllocation => "disable-allocation",
            Step::Evacuate => "evacuate-whole-disk",
            Step::VerifyEmpty => "verify-no-refs-and-allocations",
            Step::Down => "stop-io",
            Step::Removed => "mark-removed",
        }
    }
    fn resources(step: &Step) -> Vec<ResourceAccess> {
        vec![ResourceAccess::write(if matches!(step, Step::Evacuate) {
            ObjectKey::new("balance-job", "J7")
        } else {
            ObjectKey::new("disk", "disk-1")
        })]
    }
    async fn execute(&self, step: Step, context: ActivityContext) -> Outcome<()> {
        if !matches!(step, Step::Evacuate) {
            println!(
                "SHRINK: {} (simulated domain step)",
                Self::activity_name(&step)
            );
            return Ok(());
        }
        context.register_pause(Arc::new(self.0.clone()))?;
        let mut progress = self.0.progress.clone();
        let evacuation = self.0.send(Command::Start);
        tokio::pin!(evacuation);
        loop {
            tokio::select! {
                result = &mut evacuation => return result,
                _ = context.control().cancelled() => {
                    let cancel = self.0.control(Command::Cancel).await;
                    let settled = evacuation.await;
                    return match cancel {
                        Ok(()) => settled,
                        Err(error) => Err(Failure::Uncertain(format!("balance cancel outcome unknown: {error}"))),
                    };
                }
                changed = progress.changed() => {
                    changed.map_err(|_| Failure::Unavailable)?;
                    context.report_progress("evacuating J7", *progress.borrow_and_update(), Some(5));
                }
            }
        }
    }
    fn query(&self, _: ()) -> Outcome<()> {
        Ok(())
    }
}
struct Shrink {
    steps: ServiceClient<DomainSteps>,
    conditions: Arc<Conditions>,
}
#[async_trait]
impl Workflow for Shrink {
    type Output = ();
    const NAME: &'static str = "demo.shrink-whole-disk";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![ObjectKey::new("disk", "disk-1")]
    }
    fn continuation_guard(&self) -> Option<Arc<dyn ContinuationGuard>> {
        Some(self.conditions.clone())
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        for step in [
            Step::RequestShrink,
            Step::DisableAllocation,
            Step::Evacuate,
            Step::VerifyEmpty,
            Step::Down,
            Step::Removed,
        ] {
            ctx.activity(&self.steps, step).await?;
        }
        Ok(())
    }
}
struct Topology;
#[async_trait]
impl Workflow for Topology {
    type Output = ();
    const NAME: &'static str = "demo.other-disk-topology";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![ObjectKey::new("disk", "disk-2")]
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        println!("TOPO: running after shrink pause acknowledgement");
        ctx.sleep(Duration::from_millis(200)).await?;
        println!("TOPO: finished; releasing its conflict reservation");
        Ok(())
    }
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Outcome<()> {
    let (balance, host) = balancing_service();
    let config = Config::default();
    let mut driver = WorkflowDriver::new("pool-1", config.clone())?;
    let steps = driver.register("shrink-domain-adapter", DomainSteps(balance.clone()))?;
    let center = TaskCenter::new(driver, &config)?;
    let conditions = Arc::new(Conditions(AtomicBool::new(true)));
    let mut shrink = center
        .submit_shrink(Shrink {
            steps: steps.clone(),
            conditions: conditions.clone(),
        })
        .await?;
    let mut progress = balance.progress.clone();
    while *progress.borrow_and_update() == 0 {
        progress.changed().await.map_err(|_| Failure::Unavailable)?;
    }
    let mut topo = center.submit_topology(Topology).await?;
    conditions.0.store(false, Ordering::SeqCst);
    topo.result().await?;
    let mut observer = center.observe_task(shrink.id())?;
    while !matches!(observer.snapshot().state, RunState::WaitingConditions(_)) {
        observer.changed().await?;
    }
    println!(
        "SHRINK {:?}: {:?}; progress {:?}",
        shrink.id(),
        observer.snapshot().state,
        observer.snapshot().progress
    );
    conditions.0.store(true, Ordering::SeqCst);
    center.recheck(shrink.id())?;
    shrink.result().await?;
    println!(
        "SHRINK {:?}: Completed; {} Activity invocations, same evacuation Activity/job",
        shrink.id(),
        shrink.control.activities().total
    );
    center.shutdown().await?;
    drop(steps);
    drop(balance);
    host.await
        .map_err(|_| Failure::Uncertain("balance host panicked".into()))?;
    Ok(())
}
