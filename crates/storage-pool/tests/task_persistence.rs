use async_trait::async_trait;
use std::{
    collections::BTreeMap,
    future::Future,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use storage_pool::{
    task_center::{TaskCenter, persistence::*},
    *,
};
use workflow_foundation::{
    ActivityContext, ObjectKey, ResourceAccess, RunState, ServiceClient, ServiceHandler, Workflow,
    WorkflowContext, WorkflowDriver,
};

const LIMIT: Duration = Duration::from_secs(5);
async fn bounded<T>(f: impl Future<Output = T>) -> T {
    tokio::time::timeout(LIMIT, f).await.expect("timed out")
}
fn error(e: impl std::fmt::Display) -> Failure {
    Failure::Failed(e.to_string())
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct Spec {
    target: String,
    chunks: u32,
}
struct Step {
    target: String,
    chunk: u32,
}
struct Domain {
    completed: Mutex<BTreeMap<String, u32>>,
    calls: Mutex<Vec<(String, u32)>>,
    gate_first: AtomicBool,
    started: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
}
// Semaphore has no Default on all supported Tokio versions.
impl Domain {
    fn new(gated: bool) -> Arc<Self> {
        Arc::new(Self {
            completed: Mutex::default(),
            calls: Mutex::default(),
            gate_first: AtomicBool::new(gated),
            started: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
        })
    }
}
struct Moves(Arc<Domain>);
#[async_trait]
impl ServiceHandler for Moves {
    type Request = Step;
    type Reply = u32;
    type Query = String;
    type View = u32;
    fn activity_name(_: &Step) -> &'static str {
        "shrink.move-chunk"
    }
    fn resources(s: &Step) -> Vec<ResourceAccess> {
        vec![ResourceAccess::write(ObjectKey::new(
            "demo-disk",
            &s.target,
        ))]
    }
    async fn execute(&self, s: Step, _: ActivityContext) -> Outcome<u32> {
        if self.0.gate_first.swap(false, Ordering::SeqCst) {
            self.0.started.add_permits(1);
            self.0
                .release
                .acquire()
                .await
                .map_err(|_| Failure::Unavailable)?
                .forget();
        }
        let mut completed = self.0.completed.lock().unwrap();
        let current = completed.entry(s.target.clone()).or_default();
        if s.chunk != *current + 1 {
            return Err(Failure::Rejected("duplicate/out-of-order migration".into()));
        }
        *current = s.chunk;
        self.0.calls.lock().unwrap().push((s.target, s.chunk));
        Ok(s.chunk)
    }
    fn query(&self, target: String) -> Outcome<u32> {
        Ok(*self.0.completed.lock().unwrap().get(&target).unwrap_or(&0))
    }
}
struct Work {
    client: ServiceClient<Moves>,
    spec: Spec,
    from: u32,
    durable: bool,
    wait_at: Option<u32>,
}
#[async_trait]
impl Workflow for Work {
    type Output = u32;
    const NAME: &'static str = "demo.shrink";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![ObjectKey::new("demo-disk", &self.spec.target)]
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<u32> {
        for chunk in self.from + 1..=self.spec.chunks {
            let completed = ctx
                .activity(
                    &self.client,
                    Step {
                        target: self.spec.target.clone(),
                        chunk,
                    },
                )
                .await?;
            if self.durable {
                ctx.save_checkpoint(serde_json::to_vec(&completed).map_err(error)?)
                    .await?;
            }
            ctx.report_progress("migrating", completed.into(), Some(self.spec.chunks.into()));
            if self.wait_at == Some(chunk) {
                ctx.next_signal().await?;
            }
        }
        Ok(self.spec.chunks)
    }
}
struct Fixture {
    center: TaskCenter,
    client: ServiceClient<Moves>,
}
impl Fixture {
    async fn new(pool: PoolUuid, store: Arc<dyn TaskStore>, domain: Arc<Domain>) -> Self {
        let fixture = Self::uninitialized(pool, store, domain);
        fixture.center.recover_tasks().await.unwrap();
        fixture
    }
    fn uninitialized(pool: PoolUuid, store: Arc<dyn TaskStore>, domain: Arc<Domain>) -> Self {
        let config = Config {
            activity_history: 1,
            task_history: 1,
            ..Config::default()
        };
        let mut driver = WorkflowDriver::new("test-pool", config.clone()).unwrap();
        let client = driver.register("moves", Moves(domain)).unwrap();
        let center = TaskCenter::with_task_store(
            driver,
            &config,
            pool,
            PoolInstanceId::new(),
            store,
            PersistenceConfig::default(),
        )
        .unwrap();
        Self { center, client }
    }
    fn work(&self, target: &str, chunks: u32, durable: bool) -> Work {
        Work {
            client: self.client.clone(),
            spec: Spec {
                target: target.into(),
                chunks,
            },
            from: 0,
            durable,
            wait_at: None,
        }
    }
    fn register(&self) {
        let client = self.client.clone();
        self.center
            .register_recovery("shrink", 1, move |task| {
                let client = client.clone();
                async move {
                    let resume = task.resume.unwrap();
                    let spec: Spec = serde_json::from_slice(&resume.input).map_err(error)?;
                    let saved: u32 = serde_json::from_slice(&resume.checkpoint).map_err(error)?;
                    // The domain is the authority, including an effect whose checkpoint write failed.
                    let actual = client.query(spec.target.clone()).await?;
                    if actual < saved || actual > spec.chunks {
                        return Err(Failure::Uncertain(
                            "domain progress disagrees with checkpoint".into(),
                        ));
                    }
                    Ok(Work {
                        client,
                        spec,
                        from: actual,
                        durable: true,
                        wait_at: None,
                    })
                }
            })
            .unwrap();
    }
}
fn resume(spec: &Spec) -> ResumeSpec {
    ResumeSpec {
        kind: "shrink".into(),
        version: 1,
        input: serde_json::to_vec(spec).unwrap(),
        checkpoint: b"0".to_vec(),
    }
}
async fn wait_state(center: &TaskCenter, id: TaskId, predicate: impl Fn(&RunState) -> bool) {
    let mut observer = center.observe_task(id).unwrap();
    bounded(async {
        loop {
            if predicate(&observer.snapshot().execution.as_ref().unwrap().state) {
                break;
            }
            observer.changed().await.unwrap();
        }
    })
    .await;
}
async fn finished(center: &TaskCenter, id: TaskId) {
    wait_state(center, id, |s| {
        matches!(s, RunState::Completed | RunState::Failed(_))
    })
    .await;
    center.flush_history().await.unwrap();
}
async fn pending(pool: PoolUuid, store: Arc<dyn TaskStore>, domain: Arc<Domain>) -> TaskId {
    let f = Fixture::new(pool, store, domain).await;
    let mut work = f.work("a", 3, true);
    work.wait_at = Some(1);
    let spec = resume(&work.spec);
    let run = f.center.submit_resumable(work, spec).await.unwrap();
    let id = run.id();
    wait_state(&f.center, run.id(), |s| *s == RunState::WaitingSignal).await;
    f.center.shutdown().await.unwrap();
    assert_eq!(
        f.center.task_detail(id).await.unwrap().status,
        TaskStatus::Ready
    );
    id
}

#[tokio::test]
async fn history_keeps_all_activities_after_memory_eviction_without_observers() {
    let store = Arc::new(MemoryTaskStore::default());
    let f = Fixture::new(PoolUuid::new(), store.clone(), Domain::new(false)).await;
    let mut run = f.center.submit_task(f.work("a", 3, false)).await.unwrap();
    let id = run.id();
    assert_eq!(bounded(run.result()).await, Ok(3));
    f.center.flush_history().await.unwrap();
    assert_eq!(
        f.center
            .task(run.id())
            .unwrap()
            .execution
            .as_ref()
            .unwrap()
            .activities
            .entries
            .len(),
        1
    );
    let mut other = f.center.submit_task(f.work("b", 1, false)).await.unwrap();
    bounded(other.result()).await.unwrap();
    f.center.flush_history().await.unwrap();
    assert!(f.center.task(run.id()).is_err());
    let old = f.center.task_detail(id).await.unwrap();
    assert_eq!(old.status, TaskStatus::Completed);
    assert_eq!(old.executions[0].activities.len(), 3);
    assert!(
        old.executions[0]
            .activities
            .values()
            .all(|a| a.state == RecordedActivityState::Completed && a.finished_at.is_some())
    );
    assert!(f.center.retry_task(id).await.is_err());
    f.center.shutdown().await.unwrap();
}

struct UnknownTask;
#[async_trait]
impl Workflow for UnknownTask {
    type Output = ();
    const NAME: &'static str = "test.unknown-outcome";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![ObjectKey::new("demo-disk", "unknown")]
    }
    async fn run(self, _: &mut WorkflowContext) -> Outcome<()> {
        Err(Failure::Uncertain("effect requires verification".into()))
    }
}
#[tokio::test]
async fn history_only_uncertain_task_remains_visible_but_is_not_resumable() {
    let store = Arc::new(MemoryTaskStore::default());
    let pool = PoolUuid::new();
    let f = Fixture::new(pool, store.clone(), Domain::new(false)).await;
    let mut run = f.center.submit_task(UnknownTask).await.unwrap();
    let id = run.id();
    assert!(matches!(
        bounded(run.result()).await,
        Err(Failure::Uncertain(_))
    ));
    f.center.shutdown().await.unwrap();
    let new = Fixture::new(pool, store, Domain::new(false)).await;
    assert_eq!(
        new.center.task_detail(id).await.unwrap().status,
        TaskStatus::NeedsCheck
    );
    assert!(new.center.retry_task(id).await.is_err());
    assert!(
        new.center
            .task_history(TaskQuery {
                recoverable_only: true,
                ..TaskQuery::default()
            })
            .await
            .unwrap()
            .tasks
            .is_empty()
    );
    new.center.shutdown().await.unwrap();
}

#[tokio::test]
async fn paused_shrink_keeps_task_id_and_links_two_executions_across_takeover() {
    let store = Arc::new(MemoryTaskStore::default());
    let pool = PoolUuid::new();
    let domain = Domain::new(true);
    let old = Fixture::new(pool, store.clone(), domain.clone()).await;
    let work = old.work("a", 3, true);
    let spec = resume(&work.spec);
    let run = old.center.submit_resumable(work, spec).await.unwrap();
    let id = run.id();
    bounded(domain.started.acquire()).await.unwrap().forget();
    old.center.suspend_task(id).await.unwrap();
    domain.release.add_permits(1);
    bounded(old.center.wait_suspended(run.id())).await.unwrap();
    let paused = old.center.task_detail(id).await.unwrap();
    assert_eq!(paused.resume.unwrap().checkpoint, b"1");
    old.center.shutdown().await.unwrap();
    let new = Fixture::uninitialized(pool, store, domain.clone());
    new.register();
    let report = new.center.recover_tasks().await.unwrap();
    assert!(report.blocked.is_empty());
    let next = report.executions[&id];
    bounded(new.center.wait_suspended(id)).await.unwrap();
    new.center.flush_history().await.unwrap();
    assert_eq!(
        new.center.task_detail(id).await.unwrap().status,
        TaskStatus::Paused
    );
    assert!(new.center.retry_task(id).await.is_err());
    new.center.resume_task(id).await.unwrap();
    assert_eq!(new.center.task(id).unwrap().execution.unwrap().id, next);
    finished(&new.center, id).await;
    let task = new.center.task_detail(id).await.unwrap();
    assert_eq!(task.id, id);
    assert_eq!(task.status, TaskStatus::Completed);
    assert_eq!(task.executions.len(), 2);
    assert_ne!(task.executions[0].instance, task.executions[1].instance);
    assert_eq!(task.executions[0].activities.len(), 1);
    assert_eq!(task.executions[1].activities.len(), 2);
    assert_eq!(
        *domain.calls.lock().unwrap(),
        vec![("a".into(), 1), ("a".into(), 2), ("a".into(), 3)]
    );
    assert!(new.center.retry_task(id).await.is_err());
    new.center.shutdown().await.unwrap();
}

#[tokio::test]
async fn startup_automatically_reconstructs_ready_work_without_a_task_id_from_the_caller() {
    let store = Arc::new(MemoryTaskStore::default());
    let pool = PoolUuid::new();
    let domain = Domain::new(false);
    let id = pending(pool, store.clone(), domain.clone()).await;
    let new = Fixture::uninitialized(pool, store, domain.clone());
    new.register();
    let report = new.center.recover_tasks().await.unwrap();
    assert!(report.blocked.is_empty());
    assert!(report.executions.contains_key(&id));
    finished(&new.center, id).await;
    assert_eq!(
        new.center.task_detail(id).await.unwrap().status,
        TaskStatus::Completed
    );
    assert_eq!(domain.calls.lock().unwrap().len(), 3);
    assert!(new.center.recover_tasks().await.is_err());
    new.center.shutdown().await.unwrap();
}

#[tokio::test]
async fn automatic_recovery_reports_uncertain_work_and_preserves_its_claim_without_replaying() {
    let store = Arc::new(MemoryTaskStore::default());
    let pool = PoolUuid::new();
    let domain = Domain::new(false);
    let id = pending(pool, store.clone(), domain.clone()).await;
    let mut record = store.get(pool, id).unwrap().unwrap();
    let revision = record.revision;
    record.status = TaskStatus::NeedsCheck;
    record.last_error = Some(Failure::Uncertain("effect requires verification".into()));
    record.revision += 1;
    assert!(store.compare_and_set(Some(revision), &record).unwrap());
    let new = Fixture::uninitialized(pool, store, domain.clone());
    new.register();
    let report = new.center.recover_tasks().await.unwrap();
    assert!(report.executions.is_empty());
    assert_eq!(report.blocked.len(), 1);
    assert_eq!(report.blocked[0].task_id, id);
    assert!(matches!(report.blocked[0].error, Failure::Uncertain(_)));
    assert!(new.center.resume_task(id).await.is_err());
    assert!(new.center.cancel_task(id).await.is_err());
    assert_eq!(
        new.center.task_detail(id).await.unwrap().executions.len(),
        1
    );
    assert_eq!(domain.calls.lock().unwrap().len(), 1);
    assert!(
        new.center
            .submit_task(new.work("a", 3, false))
            .await
            .is_err()
    );
    let mut independent = new
        .center
        .submit_task(new.work("b", 1, false))
        .await
        .unwrap();
    bounded(independent.result()).await.unwrap();
    new.center.shutdown().await.unwrap();
}

#[tokio::test]
async fn rejected_new_request_is_history_only_and_is_never_resurrected_by_takeover() {
    let store = Arc::new(MemoryTaskStore::default());
    let pool = PoolUuid::new();
    let domain = Domain::new(false);
    let old = Fixture::new(pool, store.clone(), domain.clone()).await;
    let mut work = old.work("a", 3, true);
    work.wait_at = Some(1);
    let spec = resume(&work.spec);
    let original = old.center.submit_resumable(work, spec).await.unwrap();
    wait_state(&old.center, original.id(), |s| {
        *s == RunState::WaitingSignal
    })
    .await;
    let duplicate = old.work("a", 3, true);
    let spec = resume(&duplicate.spec);
    assert!(old.center.submit_resumable(duplicate, spec).await.is_err());
    old.center.shutdown().await.unwrap();
    let history = old.center.task_history(TaskQuery::default()).await.unwrap();
    assert_eq!(
        history
            .tasks
            .iter()
            .filter(|t| t.status == TaskStatus::Failed)
            .count(),
        1
    );
    let new = Fixture::uninitialized(pool, store, domain.clone());
    new.register();
    let report = new.center.recover_tasks().await.unwrap();
    assert!(report.blocked.is_empty());
    assert_eq!(report.executions.len(), 1);
    finished(&new.center, original.id()).await;
    assert_eq!(domain.calls.lock().unwrap().len(), 3);
    new.center.shutdown().await.unwrap();
}

#[tokio::test]
async fn recovered_claim_blocks_new_conflicting_work_until_original_task_continues() {
    let store = Arc::new(MemoryTaskStore::default());
    let pool = PoolUuid::new();
    let domain = Domain::new(false);
    let id = pending(pool, store.clone(), domain.clone()).await;
    let new = Fixture::new(pool, store, domain).await;
    let mut observer = new.center.observe_task(id).unwrap();
    let restored = observer.snapshot();
    assert_eq!(restored.id, id);
    assert!(restored.execution.is_none());
    assert!(matches!(restored.last_error, Some(Failure::Rejected(_))));
    assert!(matches!(
        new.center.submit_task(new.work("a", 3, false)).await,
        Err(Failure::Rejected(_))
    ));
    assert!(new.center.retry_task(id).await.is_err()); // missing recovery version handler
    assert_eq!(
        new.center.task_detail(id).await.unwrap().executions.len(),
        1
    );
    let mut independent = new
        .center
        .submit_task(new.work("b", 1, false))
        .await
        .unwrap();
    bounded(independent.result()).await.unwrap();
    new.register();
    new.center.retry_task(id).await.unwrap();
    bounded(async {
        while observer.snapshot().execution.is_none() {
            observer.changed().await.unwrap();
        }
    })
    .await;
    assert_eq!(observer.snapshot().id, id);
    finished(&new.center, id).await;
    assert_eq!(
        new.center.task_detail(id).await.unwrap().status,
        TaskStatus::Completed
    );
    new.center.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelling_ready_task_removes_claim_but_does_not_restart_it() {
    let store = Arc::new(MemoryTaskStore::default());
    let pool = PoolUuid::new();
    let domain = Domain::new(false);
    let id = pending(pool, store.clone(), domain.clone()).await;
    let new = Fixture::new(pool, store, domain).await;
    new.register();
    new.center.cancel_task(id).await.unwrap();
    assert_eq!(
        new.center.task_detail(id).await.unwrap().status,
        TaskStatus::Cancelled
    );
    assert!(new.center.retry_task(id).await.is_err());
    // A distinct business request is admitted after explicit cancellation.
    let mut next = new.work("a", 2, false);
    next.from = 1;
    let mut run = new.center.submit_task(next).await.unwrap();
    bounded(run.result()).await.unwrap();
    new.center.shutdown().await.unwrap();
}

#[tokio::test]
async fn concurrent_continuations_claim_exactly_one_new_execution() {
    let store = Arc::new(MemoryTaskStore::default());
    let pool = PoolUuid::new();
    let domain = Domain::new(false);
    let id = pending(pool, store.clone(), domain.clone()).await;
    let new = Fixture::new(pool, store, domain.clone()).await;
    new.register();
    let (a, b) = tokio::join!(new.center.retry_task(id), new.center.retry_task(id));
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    a.or(b).unwrap();
    finished(&new.center, id).await;
    assert_eq!(
        new.center.task_detail(id).await.unwrap().executions.len(),
        2
    );
    assert_eq!(domain.calls.lock().unwrap().len(), 3);
    new.center.shutdown().await.unwrap();
}

struct FailingCheckpoint {
    inner: MemoryTaskStore,
}
impl TaskStore for FailingCheckpoint {
    fn get(&self, p: PoolUuid, id: TaskId) -> Outcome<Option<StoredTask>> {
        self.inner.get(p, id)
    }
    fn list(&self, p: PoolUuid, q: &TaskQuery) -> Outcome<TaskPage> {
        self.inner.list(p, q)
    }
    fn compare_and_set(&self, revision: Option<u64>, t: &StoredTask) -> Outcome<bool> {
        if t.resume.as_ref().is_some_and(|r| r.checkpoint == b"1") {
            return Err(Failure::Failed("injected checkpoint failure".into()));
        }
        self.inner.compare_and_set(revision, t)
    }
}
#[tokio::test]
async fn failed_checkpoint_stops_next_effect_and_requires_business_verification() {
    let store = Arc::new(FailingCheckpoint {
        inner: MemoryTaskStore::default(),
    });
    let domain = Domain::new(false);
    let f = Fixture::new(PoolUuid::new(), store, domain.clone()).await;
    let work = f.work("a", 2, true);
    let spec = resume(&work.spec);
    let mut run = f.center.submit_resumable(work, spec).await.unwrap();
    assert!(matches!(
        bounded(run.result()).await,
        Err(Failure::Uncertain(_))
    ));
    f.center.flush_history().await.unwrap();
    let task = f.center.task_detail(run.id()).await.unwrap();
    assert_eq!(task.status, TaskStatus::NeedsCheck);
    assert_eq!(task.resume.unwrap().checkpoint, b"0");
    assert_eq!(domain.calls.lock().unwrap().len(), 1);
    assert!(f.center.cancel_task(run.id()).await.is_err());
    f.register(); // queries actual progress, then safely continues at chunk 2
    f.center.retry_task(run.id()).await.unwrap();
    finished(&f.center, run.id()).await;
    assert_eq!(
        f.center.task_detail(run.id()).await.unwrap().status,
        TaskStatus::Completed
    );
    assert_eq!(domain.calls.lock().unwrap().len(), 2);
    f.center.shutdown().await.unwrap();
}

struct SlowCheckpoint {
    inner: MemoryTaskStore,
    once: AtomicBool,
    entered: tokio::sync::Semaphore,
    gate: (Mutex<bool>, Condvar),
}
impl TaskStore for SlowCheckpoint {
    fn get(&self, p: PoolUuid, id: TaskId) -> Outcome<Option<StoredTask>> {
        self.inner.get(p, id)
    }
    fn list(&self, p: PoolUuid, q: &TaskQuery) -> Outcome<TaskPage> {
        self.inner.list(p, q)
    }
    fn compare_and_set(&self, revision: Option<u64>, t: &StoredTask) -> Outcome<bool> {
        if t.resume.as_ref().is_some_and(|r| r.checkpoint == b"1")
            && self.once.swap(false, Ordering::SeqCst)
        {
            self.entered.add_permits(1);
            let (lock, wake) = &self.gate;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = wake.wait(released).unwrap();
            }
        }
        self.inner.compare_and_set(revision, t)
    }
}
#[tokio::test]
async fn slow_database_does_not_block_other_workflows_in_the_taskcenter() {
    let store = Arc::new(SlowCheckpoint {
        inner: MemoryTaskStore::default(),
        once: AtomicBool::new(true),
        entered: tokio::sync::Semaphore::new(0),
        gate: (Mutex::new(false), Condvar::new()),
    });
    let domain = Domain::new(false);
    let f = Fixture::new(PoolUuid::new(), store.clone(), domain).await;
    let work = f.work("a", 2, true);
    let spec = resume(&work.spec);
    let mut slow = f.center.submit_resumable(work, spec).await.unwrap();
    bounded(store.entered.acquire()).await.unwrap().forget();
    let mut other = f.center.submit_task(f.work("b", 1, false)).await.unwrap();
    let result = tokio::time::timeout(LIMIT, other.result()).await;
    *store.gate.0.lock().unwrap() = true;
    store.gate.1.notify_all();
    assert_eq!(result.unwrap(), Ok(1));
    bounded(slow.result()).await.unwrap();
    f.center.shutdown().await.unwrap();
}

#[tokio::test]
async fn pool_scope_pagination_and_adapter_revision_contract() {
    let store = Arc::new(MemoryTaskStore::default());
    let pool = PoolUuid::new();
    let a = Fixture::new(pool, store.clone(), Domain::new(false)).await;
    let b = Fixture::new(PoolUuid::new(), store.clone(), Domain::new(false)).await;
    for target in ["a", "b", "c"] {
        let mut run = a
            .center
            .submit_task(a.work(target, 1, false))
            .await
            .unwrap();
        bounded(run.result()).await.unwrap();
    }
    a.center.flush_history().await.unwrap();
    let mut cursor = None;
    let mut ids = Vec::new();
    loop {
        let page = a
            .center
            .task_history(TaskQuery {
                before: cursor,
                limit: 1,
                ..TaskQuery::default()
            })
            .await
            .unwrap();
        ids.extend(page.tasks.iter().map(|t| t.id));
        match page.next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 3);
    assert!(b.center.task_detail(ids[0]).await.is_err());
    assert!(
        b.center
            .task_history(TaskQuery {
                before: Some(format!("{pool}/foreign")),
                ..TaskQuery::default()
            })
            .await
            .is_err()
    );
    let mut task = store.get(pool, ids[0]).unwrap().unwrap();
    task.revision += 1;
    assert!(
        store
            .compare_and_set(Some(task.revision - 1), &task)
            .unwrap()
    );
    assert!(
        !store
            .compare_and_set(Some(task.revision - 1), &task)
            .unwrap()
    );
    a.center.shutdown().await.unwrap();
    b.center.shutdown().await.unwrap();
}

#[tokio::test]
async fn configured_store_records_builtin_pool_tasks_and_survives_manager_recreation() {
    use storage_pool::adapters::{MemoryMetadata, SimulatedDataPlane};
    let store = Arc::new(MemoryTaskStore::default());
    let make_manager = || {
        PoolManager::new(
            Config::default(),
            Arc::new(MemoryMetadata::default()),
            Arc::new(SimulatedDataPlane),
        )
        .with_task_store(store.clone(), PersistenceConfig::default())
        .unwrap()
    };
    let request = CreatePool::new(PoolId::new("pool-a").unwrap(), PoolName::new("a").unwrap());
    let manager = make_manager();
    let pool = manager.create(request.clone()).unwrap();
    pool.takeover(vec![]).await.unwrap();
    let mut run = pool
        .attach_node(NodeId::new("node-1").unwrap(), 1, true)
        .await
        .unwrap();
    let id = run.id();
    bounded(run.result()).await.unwrap();
    let event = TopologyEvent::new(
        1,
        2,
        TopologyChange::NodeConnectivity {
            node: NodeId::new("node-1").unwrap(),
            connected: false,
        },
    );
    let source = event.id;
    let receipt = manager.dispatch(event).await.unwrap();
    let report = manager
        .gather(
            &receipt,
            WaitFor::Completion,
            tokio::time::Instant::now() + LIMIT,
        )
        .await;
    assert_eq!(report.outcome, FanoutOutcome::Succeeded);
    pool.flush_task_history().await.unwrap();
    assert!(
        pool.task_history(TaskQuery::default())
            .await
            .unwrap()
            .tasks
            .iter()
            .any(|t| t.source_event == Some(source))
    );
    manager.shutdown().await.unwrap();
    let next_manager = make_manager();
    let next_pool = next_manager.create(request).unwrap();
    next_pool.takeover(vec![]).await.unwrap();
    let task = next_pool.task_detail(id).await.unwrap();
    assert_eq!(task.status, TaskStatus::Completed);
    assert!(!task.executions[0].activities.is_empty());
    assert!(next_pool.retry_task(id).await.is_err());
    next_manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn recovery_factory_cannot_change_the_original_task_objects() {
    let store = Arc::new(MemoryTaskStore::default());
    let pool = PoolUuid::new();
    let domain = Domain::new(false);
    let id = pending(pool, store.clone(), domain.clone()).await;
    let new = Fixture::new(pool, store, domain.clone()).await;
    let client = new.client.clone();
    new.center
        .register_recovery("shrink", 1, move |_| {
            let client = client.clone();
            async move {
                Ok(Work {
                    client,
                    spec: Spec {
                        target: "different-disk".into(),
                        chunks: 3,
                    },
                    from: 1,
                    durable: true,
                    wait_at: None,
                })
            }
        })
        .unwrap();
    assert!(matches!(
        new.center.retry_task(id).await,
        Err(Failure::Rejected(_))
    ));
    assert_eq!(
        new.center.task_detail(id).await.unwrap().executions.len(),
        1
    );
    assert_eq!(domain.calls.lock().unwrap().len(), 1);
    new.center.shutdown().await.unwrap();
}

#[cfg(feature = "kv-redb")]
#[tokio::test]
async fn redb_history_survives_closing_and_reopening_the_database() {
    let path = std::env::temp_dir().join(format!("task-center-{}.redb", uuid::Uuid::new_v4()));
    let pool = PoolUuid::new();
    let store = Arc::new(RedbTaskStore::open(&path).unwrap());
    let f = Fixture::new(pool, store.clone(), Domain::new(false)).await;
    let mut run = f.center.submit_task(f.work("a", 3, false)).await.unwrap();
    bounded(run.result()).await.unwrap();
    f.center.flush_history().await.unwrap();
    let task = f
        .center
        .task_history(TaskQuery::default())
        .await
        .unwrap()
        .tasks
        .remove(0);
    f.center.shutdown().await.unwrap();
    f.center.close_task_store().await.unwrap();
    drop(run);
    drop(f);
    drop(store);
    let reopened = RedbTaskStore::open(&path).unwrap();
    let restored = reopened.get(pool, task.id).unwrap().unwrap();
    assert_eq!(restored.status, TaskStatus::Completed);
    assert_eq!(restored.executions[0].activities.len(), 3);
    assert!(
        reopened
            .list(
                pool,
                &TaskQuery {
                    recoverable_only: true,
                    ..TaskQuery::default()
                }
            )
            .unwrap()
            .tasks
            .is_empty()
    );
    drop(reopened);
    std::fs::remove_file(path).unwrap();
}
