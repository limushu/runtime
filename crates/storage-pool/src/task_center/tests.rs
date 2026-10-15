use super::*;
use async_trait::async_trait;
use std::time::Duration;
use workflow_foundation::{Signal, WorkflowContext, WorkflowDriver};

const LIMIT: Duration = Duration::from_secs(3);
fn key(s: &str) -> ObjectKey {
    ObjectKey::new("test", s)
}
fn volume(s: &str) -> OperationKind {
    OperationKind::CreateVolume(VolumeId::new(s).unwrap())
}
fn operation(kind: OperationKind, keys: &[&str]) -> PoolOperation {
    PoolOperation {
        kind,
        objects: keys.iter().map(|s| key(s)).collect(),
    }
}
struct Work {
    operation: PoolOperation,
    started: mpsc::UnboundedSender<WorkflowId>,
    uncertain: bool,
}
impl PoolWorkflow for Work {
    fn operation(&self) -> PoolOperation {
        self.operation.clone()
    }
}
#[async_trait]
impl Workflow for Work {
    type Output = ();
    const NAME: &'static str = "test.task-center";
    fn objects(&self) -> Vec<ObjectKey> {
        self.operation.objects.clone()
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        self.started.send(ctx.id()).unwrap();
        ctx.next_signal().await?;
        if self.uncertain {
            Err(Failure::Uncertain("unknown external effect".into()))
        } else {
            Ok(())
        }
    }
}
struct Fixture {
    center: TaskCenter,
    started: mpsc::UnboundedSender<WorkflowId>,
    events: mpsc::UnboundedReceiver<WorkflowId>,
}
impl Fixture {
    fn new(limit: usize) -> Self {
        let config = Config {
            max_workflows: limit,
            ..Config::default()
        };
        let runtime = WorkflowDriver::new("test", config.clone()).unwrap();
        let center = TaskCenter::start(runtime, &config);
        let (started, events) = mpsc::unbounded_channel();
        Self {
            center,
            started,
            events,
        }
    }
    async fn submit(&self, operation: PoolOperation, uncertain: bool) -> Outcome<Run<()>> {
        tokio::time::timeout(
            LIMIT,
            self.center.submit(Work {
                operation,
                started: self.started.clone(),
                uncertain,
            }),
        )
        .await
        .unwrap()
    }
    async fn next(&mut self) -> WorkflowId {
        tokio::time::timeout(LIMIT, self.events.recv())
            .await
            .unwrap()
            .unwrap()
    }
    async fn close(&mut self) {
        tokio::time::timeout(LIMIT, self.center.shutdown())
            .await
            .unwrap()
            .unwrap();
    }
}
async fn result(run: &mut Run<()>) -> Outcome<()> {
    tokio::time::timeout(LIMIT, run.result()).await.unwrap()
}
async fn finish(run: &mut Run<()>) -> Outcome<()> {
    run.control.signal(Signal::new("finish", "")).await.unwrap();
    result(run).await
}
#[test]
fn business_conflicts_have_explicit_names_and_directions() {
    use ConflictDecision::*;
    let n = NodeId::new("n").unwrap();
    let d = DiskId::new("d").unwrap();
    let cases = [
        (
            OperationKind::OfflineNode {
                id: n.clone(),
                sequence: 2,
            },
            OperationKind::OnlineDisk(d.clone()),
            CancelThenWait,
            "node-down-cancels-disk-up",
        ),
        (
            OperationKind::OnlineNode {
                id: n.clone(),
                sequence: 2,
            },
            OperationKind::OfflineDisk(d.clone()),
            CancelThenWait,
            "node-up-cancels-disk-down",
        ),
        (
            OperationKind::OfflineDisk(d.clone()),
            OperationKind::OnlineDisk(d.clone()),
            CancelThenWait,
            "disk-down-cancels-disk-up",
        ),
        (
            OperationKind::OnlineDisk(d.clone()),
            OperationKind::OfflineDisk(d.clone()),
            Wait,
            "overlapping-objects",
        ),
        (
            OperationKind::OfflineDisk(DiskId::new("other").unwrap()),
            OperationKind::OnlineDisk(d.clone()),
            Wait,
            "overlapping-objects",
        ),
        (
            OperationKind::ObserveNode {
                id: n.clone(),
                sequence: 3,
                connected: false,
            },
            OperationKind::LoadNode(n.clone()),
            CancelThenWait,
            "disconnected-cancels-load",
        ),
    ];
    for (incoming, existing, decision, reason) in cases {
        assert_eq!(
            TaskCenter::conflict(&operation(incoming, &["n"]), &operation(existing, &["n"])),
            Conflict { decision, reason }
        );
    }
    let same = operation(OperationKind::OnlineDisk(d.clone()), &["d"]);
    assert_eq!(
        TaskCenter::conflict(&same, &operation(OperationKind::OfflineDisk(d), &["other"])).decision,
        Independent
    );
    assert!(matches!(
        TaskCenter::conflict(&same, &same).decision,
        Reject(_)
    ));
    let newer = operation(
        OperationKind::OnlineNode {
            id: n.clone(),
            sequence: 10,
        },
        &["n"],
    );
    let stale = operation(OperationKind::OfflineNode { id: n, sequence: 9 }, &["n"]);
    assert_eq!(
        TaskCenter::conflict(&stale, &newer).reason,
        "stale-node-guard"
    );
    assert_eq!(
        TaskCenter::conflict(&newer, &stale).decision,
        CancelThenWait
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejection_of_one_conflict_prevents_cancelling_another() {
    let mut f = Fixture::new(8);
    let d = DiskId::new("d").unwrap();
    let n = NodeId::new("n").unwrap();
    let mut online = f
        .submit(operation(OperationKind::OnlineDisk(d), &["n"]), false)
        .await
        .unwrap();
    assert_eq!(f.next().await, online.id());
    // A connected observation waits for disk online, without cancelling it.
    let mut newer = f
        .submit(
            operation(
                OperationKind::ObserveNode {
                    id: n.clone(),
                    sequence: 10,
                    connected: true,
                },
                &["n"],
            ),
            false,
        )
        .await
        .unwrap();
    let incoming = operation(OperationKind::OfflineNode { id: n, sequence: 9 }, &["n"]);
    assert!(matches!(
        f.submit(incoming, false).await,
        Err(Failure::Rejected(_))
    ));
    assert!(!online.control.cancellation_requested());
    assert!(!newer.control.cancellation_requested());
    assert_eq!(f.center.tasks().len(), 2);
    f.close().await;
    assert_eq!(result(&mut online).await, Err(Failure::Cancelled));
    assert_eq!(result(&mut newer).await, Err(Failure::Cancelled));
}

#[tokio::test]
async fn capacity_rejection_never_cancels_existing_work() {
    let mut f = Fixture::new(1);
    let d = DiskId::new("d").unwrap();
    let mut online = f
        .submit(
            operation(OperationKind::OnlineDisk(d.clone()), &["d"]),
            false,
        )
        .await
        .unwrap();
    assert_eq!(f.next().await, online.id());
    assert!(matches!(
        f.submit(operation(OperationKind::OfflineDisk(d), &["d"]), false)
            .await,
        Err(Failure::Busy)
    ));
    assert!(!online.control.cancellation_requested());
    finish(&mut online).await.unwrap();
    f.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fifo_conflicts_do_not_block_disjoint_work_and_cancelled_waiter_is_removed() {
    let mut f = Fixture::new(8);
    let mut a = f
        .submit(operation(volume("a"), &["a"]), false)
        .await
        .unwrap();
    assert_eq!(f.next().await, a.id());
    let mut ab = f
        .submit(operation(volume("ab"), &["a", "b"]), false)
        .await
        .unwrap();
    let mut b = f
        .submit(operation(volume("b"), &["b"]), false)
        .await
        .unwrap();
    let mut c = f
        .submit(operation(volume("c"), &["c"]), false)
        .await
        .unwrap();
    assert_eq!(f.next().await, c.id());
    assert_eq!(
        f.center.task(ab.id()).unwrap().state,
        RunState::WaitingObjects
    );
    assert_eq!(
        f.center.task(b.id()).unwrap().state,
        RunState::WaitingObjects
    );
    assert_eq!(f.center.scheduling().queued, 2);
    assert_eq!(f.center.scheduling().running, 2);
    ab.control.cancel();
    assert_eq!(result(&mut ab).await, Err(Failure::Cancelled));
    assert_eq!(f.next().await, b.id());
    finish(&mut b).await.unwrap();
    finish(&mut a).await.unwrap();
    finish(&mut c).await.unwrap();
    f.close().await;
}

#[tokio::test]
async fn uncertain_workflow_without_activity_fences_only_its_objects() {
    let mut f = Fixture::new(8);
    let mut a = f
        .submit(operation(volume("a"), &["a"]), true)
        .await
        .unwrap();
    assert_eq!(f.next().await, a.id());
    let mut ab = f
        .submit(operation(volume("ab"), &["a", "b"]), false)
        .await
        .unwrap();
    assert!(matches!(finish(&mut a).await, Err(Failure::Uncertain(_))));
    assert!(matches!(result(&mut ab).await, Err(Failure::Rejected(_))));
    // Rejected waiter must not spread the fence from a to b.
    let mut b = f
        .submit(operation(volume("b"), &["b"]), false)
        .await
        .unwrap();
    assert_eq!(f.next().await, b.id());
    let mut retry = f
        .submit(operation(volume("retry"), &["a"]), false)
        .await
        .unwrap();
    assert!(matches!(
        result(&mut retry).await,
        Err(Failure::Rejected(_))
    ));
    assert_eq!(f.center.scheduling().fenced, 1);
    finish(&mut b).await.unwrap();
    f.close().await;
}

#[tokio::test]
async fn dropped_handle_keeps_ownership_and_observer_keeps_conflict_explanation() {
    let mut f = Fixture::new(8);
    let a = f
        .submit(operation(volume("a"), &["a"]), false)
        .await
        .unwrap();
    let id = a.id();
    assert_eq!(f.next().await, id);
    drop(a);
    let mut b = f
        .submit(operation(volume("b"), &["a"]), false)
        .await
        .unwrap();
    let observer = f.center.observe_task(b.id()).unwrap();
    assert_eq!(observer.snapshot().conflicts[0].existing, id);
    assert_eq!(observer.snapshot().state, RunState::WaitingObjects);
    f.center.cancel(id).unwrap();
    assert_eq!(f.next().await, b.id());
    finish(&mut b).await.unwrap();
    assert_eq!(observer.snapshot().state, RunState::Completed);
    assert_eq!(
        observer.snapshot().conflicts[0].conflict.reason,
        "overlapping-objects"
    );
    f.close().await;
}

#[tokio::test]
async fn fenced_replacement_does_not_cancel_unrelated_live_work() {
    let mut f = Fixture::new(8);
    let mut uncertain = f
        .submit(operation(volume("unknown"), &["a"]), true)
        .await
        .unwrap();
    assert_eq!(f.next().await, uncertain.id());
    assert!(matches!(
        finish(&mut uncertain).await,
        Err(Failure::Uncertain(_))
    ));
    let mut live = f
        .submit(
            operation(OperationKind::OnlineDisk(DiskId::new("d").unwrap()), &["b"]),
            false,
        )
        .await
        .unwrap();
    assert_eq!(f.next().await, live.id());
    let mut replacement = f
        .submit(
            operation(
                OperationKind::OfflineNode {
                    id: NodeId::new("n").unwrap(),
                    sequence: 2,
                },
                &["a", "b"],
            ),
            false,
        )
        .await
        .unwrap();
    assert!(matches!(
        result(&mut replacement).await,
        Err(Failure::Rejected(_))
    ));
    assert!(!live.control.cancellation_requested());
    finish(&mut live).await.unwrap();
    f.close().await;
}
