use async_trait::async_trait;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;
use workflow_foundation::{persistence::*, *};

async fn bounded<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), f)
        .await
        .expect("history operation stalled")
}
async fn driver(store: Arc<dyn TaskStore>, epoch: u64, config: Config) -> WorkflowDriver {
    let mut driver = WorkflowDriver::new("pool", config)
        .unwrap()
        .with_task_ids(7, TaskIds::for_leader(epoch).unwrap())
        .unwrap();
    driver.enable_persistence(store).await.unwrap();
    assert!(driver.restore().await.unwrap().is_empty());
    driver
}
fn host(mut driver: WorkflowDriver) -> (CancellationToken, tokio::task::JoinHandle<()>) {
    let stop = CancellationToken::new();
    let end = stop.clone();
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = end.cancelled() => break,
                _ = driver.tick() => {},
            }
        }
        driver.shutdown().await.unwrap();
    });
    (stop, task)
}
struct Work {
    count: Arc<AtomicUsize>,
    wait: bool,
    fail: bool,
}
#[async_trait]
impl Workflow for Work {
    const NAME: &'static str = "history.work";
    type Output = ();
    fn objects(&self) -> Vec<ObjectKey> {
        vec![ObjectKey::new("disk", "a")]
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        self.count.fetch_add(1, Ordering::SeqCst);
        ctx.report_details(&serde_json::json!({"event":"inspect disk a"}))?;
        ctx.report_progress("inspected", 1, Some(1));
        if self.wait {
            ctx.next_signal().await?;
        }
        if self.fail {
            Err(Failure::Failed("inspection failed".into()))
        } else {
            Ok(())
        }
    }
}
fn work(count: &Arc<AtomicUsize>) -> Work {
    Work {
        count: count.clone(),
        wait: false,
        fail: false,
    }
}
async fn observed(
    view: &ExecutionView,
    id: TaskId,
    check: impl Fn(&SavedTask) -> bool,
) -> SavedTask {
    bounded(async {
        loop {
            let task = view.history_task(id).await.unwrap();
            if check(&task) {
                return task;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
}

#[tokio::test]
async fn ordinary_history_survives_eviction_file_reopen_and_leader_change_without_a_definition() {
    let path = std::env::temp_dir().join(format!("task-history-{}", uuid::Uuid::new_v4()));
    let count = Arc::new(AtomicUsize::new(0));
    let id;
    {
        let store = Arc::new(FileTaskStore::open(&path).unwrap());
        let mut driver = driver(
            store,
            1,
            Config {
                task_history: 0,
                ..Config::default()
            },
        )
        .await;
        let view = driver.view();
        let mut run = driver.submit(work(&count)).unwrap();
        id = run.id();
        let (stop, join) = host(driver);
        bounded(run.result()).await.unwrap();
        bounded(async {
            while view.task(id).is_ok() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        let record = view.history_task(id).await.unwrap();
        assert!(!record.recoverable);
        assert_eq!(record.outcome, StoredOutcome::Completed);
        assert!(record.observation.created_at <= record.observation.started_at);
        assert!(record.observation.started_at <= record.observation.finished_at);
        assert_eq!(record.observation.progress.completed, 1);
        assert_eq!(record.details["event"], "inspect disk a");
        assert_eq!(record.objects, vec![ObjectKey::new("disk", "a")]);
        stop.cancel();
        bounded(join).await.unwrap();
    }
    {
        let store = Arc::new(FileTaskStore::open(&path).unwrap());
        let mut next = driver(store, 2, Config::default()).await;
        assert!(next.is_idle());
        assert!(next.view().tasks().is_empty());
        let record = next.view().history_task(id).await.unwrap();
        assert_eq!(record.outcome, StoredOutcome::Completed);
        assert_eq!(record.id, id);
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert!(next.view().resume(id).is_err());
        assert_eq!(
            next.view()
                .history(TaskQuery::default())
                .await
                .unwrap()
                .tasks
                .len(),
            1
        );
        next.shutdown().await.unwrap();
    }
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn crash_marks_unfinished_ordinary_task_interrupted_without_reexecuting_or_faking_completion()
{
    let store = Arc::new(MemoryTaskStore::default());
    let count = Arc::new(AtomicUsize::new(0));
    let mut old = driver(store.clone(), 1, Config::default()).await;
    let view = old.view();
    let mut request = work(&count);
    request.wait = true;
    let mut run = old.submit(request).unwrap();
    let id = run.id();
    let (_, task) = host(old);
    bounded(run.accepted()).await.unwrap();
    observed(&view, id, |r| {
        r.observation.state == Some(RunState::WaitingSignal)
            && r.observation.progress.completed == 1
    })
    .await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(matches!(
        bounded(run.result()).await,
        Err(Failure::Uncertain(_))
    ));
    let stale = store.get(id).unwrap().unwrap();
    assert_eq!(stale.outcome, StoredOutcome::Pending);
    let mut next = driver(store.clone(), 2, Config::default()).await;
    let recovered = next.view().history_task(id).await.unwrap();
    assert_eq!(recovered.outcome, StoredOutcome::Interrupted);
    assert!(
        recovered.observation.finished_at.is_none(),
        "crash gives no completion timestamp"
    );
    assert_eq!(recovered.observation.progress.completed, 1);
    assert_eq!(recovered.details, stale.details);
    assert!(next.view().tasks().is_empty());
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert!(next.view().cancel(id).is_err());
    let mut late = stale.clone();
    late.revision += 1;
    assert!(store.save(late, Some(stale.revision)).is_err());
    next.shutdown().await.unwrap();
}

#[tokio::test]
async fn failure_user_cancel_and_host_interruption_remain_distinct_in_history() {
    for mode in ["failure", "cancel", "shutdown"] {
        let store = Arc::new(MemoryTaskStore::default());
        let count = Arc::new(AtomicUsize::new(0));
        let mut driver = driver(store, 1, Config::default()).await;
        let view = driver.view();
        let mut request = work(&count);
        request.fail = mode == "failure";
        request.wait = !request.fail;
        let mut run = driver.submit(request).unwrap();
        let id = run.id();
        let (stop, join) = host(driver);
        bounded(run.accepted()).await.unwrap();
        if mode != "failure" {
            observed(&view, id, |r| {
                r.observation.state == Some(RunState::WaitingSignal)
            })
            .await;
            if mode == "cancel" {
                run.control.cancel();
            } else {
                stop.cancel();
            }
        }
        assert!(bounded(run.result()).await.is_err());
        let record = view.history_task(id).await.unwrap();
        match mode {
            "failure" => assert!(matches!(record.outcome, StoredOutcome::Failed(_))),
            "cancel" => {
                assert_eq!(record.outcome, StoredOutcome::Cancelled);
                assert!(record.cancel_requested);
            }
            _ => {
                assert_eq!(record.outcome, StoredOutcome::Interrupted);
                assert!(!record.cancel_requested);
            }
        }
        assert!(
            record.observation.finished_at.is_some(),
            "orderly settlement has a finish timestamp"
        );
        stop.cancel();
        bounded(join).await.unwrap();
    }
}

#[tokio::test]
async fn history_pages_are_pool_scoped_filtered_and_do_not_fill_execution_capacity() {
    let store = Arc::new(MemoryTaskStore::default());
    for pool in [7, 8] {
        store.claim(pool, 1).unwrap();
        for sequence in 1..=5 {
            let business = if sequence % 2 == 1 {
                "inspect"
            } else {
                "other"
            };
            store
                .save(
                    SavedTask {
                        id: format!("1/{pool:032x}/{business}/{sequence}")
                            .parse()
                            .unwrap(),
                        recoverable: false,
                        observation: Default::default(),
                        version: 0,
                        input: serde_json::Value::Null,
                        checkpoint: serde_json::Value::Null,
                        objects: vec![],
                        metadata: serde_json::Value::Null,
                        details: serde_json::Value::Null,
                        revision: 0,
                        owner_epoch: 1,
                        user_paused: false,
                        cancel_requested: false,
                        outcome: StoredOutcome::Completed,
                    },
                    None,
                )
                .unwrap();
        }
    }
    let mut driver = driver(
        store,
        2,
        Config {
            max_workflows: 1,
            ..Config::default()
        },
    )
    .await;
    let view = driver.view();
    assert!(view.tasks().is_empty());
    let first = view
        .history(TaskQuery {
            limit: 2,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        first
            .tasks
            .iter()
            .map(|t| t.id.sequence)
            .collect::<Vec<_>>(),
        vec![5, 4]
    );
    let second = view
        .history(TaskQuery {
            before: first.next,
            limit: 2,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        second
            .tasks
            .iter()
            .map(|t| t.id.sequence)
            .collect::<Vec<_>>(),
        vec![3, 2]
    );
    let last = view
        .history(TaskQuery {
            before: second.next,
            limit: 2,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(last.tasks[0].id.sequence, 1);
    assert!(last.next.is_none());
    let filtered = view
        .history(TaskQuery {
            business: Some("inspect".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        filtered
            .tasks
            .iter()
            .map(|t| t.id.sequence)
            .collect::<Vec<_>>(),
        vec![5, 3, 1]
    );
    let foreign: TaskId = "1/00000000000000000000000000000008/inspect/1"
        .parse()
        .unwrap();
    assert!(view.history_task(foreign).await.is_err());
    assert!(
        view.history(TaskQuery {
            before: Some(foreign),
            ..Default::default()
        })
        .await
        .is_err()
    );
    for limit in [0, 1001] {
        assert!(
            view.history(TaskQuery {
                limit,
                ..Default::default()
            })
            .await
            .is_err()
        );
    }
    driver.shutdown().await.unwrap();
}

struct PolicyWork {
    policy: Persistence,
}
#[async_trait]
impl Workflow for PolicyWork {
    const NAME: &'static str = "history.policy";
    type Output = ();
    fn persistence(&self) -> Outcome<Persistence> {
        Ok(self.policy.clone())
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        if self.policy == Persistence::History {
            assert!(matches!(
                ctx.save_checkpoint(&1).await,
                Err(Failure::Rejected(_))
            ));
        }
        Ok(())
    }
}
#[tokio::test]
async fn history_policy_does_not_grant_recovery_and_can_be_disabled_without_disabling_durable_tasks()
 {
    for record_history in [false, true] {
        let store = Arc::new(MemoryTaskStore::default());
        let mut driver = driver(
            store.clone(),
            1,
            Config {
                record_history,
                ..Config::default()
            },
        )
        .await;
        let mut memory = driver
            .submit(PolicyWork {
                policy: Persistence::Memory,
            })
            .unwrap();
        let mut history = driver
            .submit(PolicyWork {
                policy: Persistence::History,
            })
            .unwrap();
        let mut durable = driver
            .submit(PolicyWork {
                policy: Persistence::durable(1, &()).unwrap(),
            })
            .unwrap();
        let (stop, join) = host(driver);
        bounded(memory.result()).await.unwrap();
        bounded(history.result()).await.unwrap();
        bounded(durable.result()).await.unwrap();
        assert!(store.get(memory.id()).unwrap().is_none());
        assert_eq!(store.get(history.id()).unwrap().is_some(), record_history);
        assert!(store.get(durable.id()).unwrap().unwrap().recoverable);
        stop.cancel();
        bounded(join).await.unwrap();
    }
}

struct FailedStore;
impl TaskStore for FailedStore {
    fn claim(&self, _: u128, _: u64) -> Outcome<()> {
        Ok(())
    }
    fn list(&self, _: u128) -> Outcome<Vec<SavedTask>> {
        Ok(vec![])
    }
    fn save(&self, _: SavedTask, _: Option<u64>) -> Outcome<()> {
        Err(Failure::Failed("history write failed".into()))
    }
    fn query(&self, _: u128, _: &TaskQuery) -> Outcome<TaskPage> {
        Err(Failure::Unavailable)
    }
}
#[tokio::test]
async fn failed_history_acceptance_does_not_run_business_and_query_errors_are_not_empty_pages() {
    let count = Arc::new(AtomicUsize::new(0));
    let mut driver = driver(Arc::new(FailedStore), 1, Config::default()).await;
    let view = driver.view();
    let mut run = driver.submit(work(&count)).unwrap();
    let (stop, join) = host(driver);
    assert!(bounded(run.accepted()).await.is_err());
    assert!(bounded(run.result()).await.is_err());
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert!(matches!(
        view.history(TaskQuery::default()).await,
        Err(Failure::Unavailable)
    ));
    stop.cancel();
    bounded(join).await.unwrap();
}

#[test]
fn pre_history_records_remain_recoverable_when_deserialized() {
    let record:SavedTask = serde_json::from_value(serde_json::json!({
        "id":"1/00000000000000000000000000000007/legacy/1", "version":1,
        "input":null,"checkpoint":{"next":2},"objects":[],"metadata":null,"details":null,
        "revision":0,"owner_epoch":1,"user_paused":false,"cancel_requested":false,"outcome":"Pending"
    })).unwrap();
    assert!(record.recoverable);
    assert_eq!(record.checkpoint["next"], 2);
    assert!(record.observation.created_at.is_none());
}

// A user KV adapter implementing only the original three methods automatically
// gets working get/query/unfinished fallbacks; efficient indexes remain optional.
struct ExistingAdapter(MemoryTaskStore);
impl TaskStore for ExistingAdapter {
    fn claim(&self, pool: u128, epoch: u64) -> Outcome<()> {
        self.0.claim(pool, epoch)
    }
    fn list(&self, pool: u128) -> Outcome<Vec<SavedTask>> {
        self.0.list(pool)
    }
    fn save(&self, task: SavedTask, previous: Option<u64>) -> Outcome<()> {
        self.0.save(task, previous)
    }
}
#[tokio::test]
async fn existing_task_store_adapter_supports_history_without_new_required_methods() {
    let store = Arc::new(ExistingAdapter(MemoryTaskStore::default()));
    let count = Arc::new(AtomicUsize::new(0));
    let mut primary = driver(store.clone(), 1, Config::default()).await;
    let mut run = primary.submit(work(&count)).unwrap();
    let id = run.id();
    let (stop, join) = host(primary);
    bounded(run.result()).await.unwrap();
    stop.cancel();
    bounded(join).await.unwrap();
    let mut standby = driver(store, 2, Config::default()).await;
    assert_eq!(
        standby.view().history_task(id).await.unwrap().outcome,
        StoredOutcome::Completed
    );
    let page = standby.view().history(TaskQuery::default()).await.unwrap();
    assert_eq!(page.tasks.len(), 1);
    assert_eq!(page.tasks[0].id, id);
    standby.shutdown().await.unwrap();
}
