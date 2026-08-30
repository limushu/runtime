use async_trait::async_trait;
use control_runtime::{
    spawn_service, ExecutionClass, ObjectActivity, ObjectDecision, ObjectKey, Router, RuntimeError,
    RuntimeResult, Service, ServiceId, ServiceRequest, StateCell, WorkflowContext, WorkflowMeta,
};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone)]
enum LeafRequest {
    Run(&'static str),
    Stats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeafReply {
    Done,
    Stats { started: usize, stable: usize },
}

impl ServiceRequest for LeafRequest {
    type Response = LeafReply;

    fn service_id() -> ServiceId {
        ServiceId::new("contract.leaf")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeafWorkflow {
    Run,
}

#[derive(Debug, Default)]
struct LeafStats {
    started: usize,
    stable: usize,
}

#[derive(Debug, Default)]
struct LeafService {
    stats: StateCell<LeafStats>,
}

#[async_trait]
impl Service for LeafService {
    type Request = LeafRequest;
    type WorkflowKind = LeafWorkflow;

    fn classify(&self, request: &Self::Request) -> ExecutionClass<Self::WorkflowKind> {
        match request {
            LeafRequest::Run(key) => ExecutionClass::Workflow(WorkflowMeta::object(
                ObjectKey::new(*key),
                LeafWorkflow::Run,
                format!("run {key}"),
            )),
            LeafRequest::Stats => ExecutionClass::Inline,
        }
    }

    fn decide(
        &self,
        _context: &WorkflowContext,
        _request: &Self::Request,
        _incoming: &WorkflowMeta<Self::WorkflowKind>,
        activity: &ObjectActivity<Self::WorkflowKind>,
    ) -> RuntimeResult<ObjectDecision<LeafReply>> {
        Ok(match activity {
            ObjectActivity::Idle => ObjectDecision::Start,
            ObjectActivity::Pending { .. } => ObjectDecision::JoinPending,
            ObjectActivity::Running { .. } => ObjectDecision::JoinExisting,
            ObjectActivity::Cancelling { .. } => ObjectDecision::JoinReplacement,
        })
    }

    async fn handle(
        &self,
        request: Self::Request,
        context: WorkflowContext,
    ) -> RuntimeResult<LeafReply> {
        match request {
            LeafRequest::Run(_) => {
                self.stats.update(|stats| stats.started += 1);
                tokio::select! {
                    _ = context.cancellation().requested() => {
                        // Represents settling an already issued downstream effect.
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        self.stats.update(|stats| stats.stable += 1);
                        Err(RuntimeError::Cancelled)
                    }
                    _ = tokio::time::sleep(Duration::from_millis(50)) => Ok(LeafReply::Done),
                }
            }
            LeafRequest::Stats => Ok(self.stats.read(|stats| LeafReply::Stats {
                started: stats.started,
                stable: stats.stable,
            })),
        }
    }
}

#[derive(Debug, Clone)]
enum ParentRequest {
    Run,
}

impl ServiceRequest for ParentRequest {
    type Response = LeafReply;

    fn service_id() -> ServiceId {
        ServiceId::new("contract.parent")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParentWorkflow {
    Run,
}

struct ParentService {
    router: Router,
}

#[async_trait]
impl Service for ParentService {
    type Request = ParentRequest;
    type WorkflowKind = ParentWorkflow;

    fn classify(&self, _request: &Self::Request) -> ExecutionClass<Self::WorkflowKind> {
        ExecutionClass::Workflow(WorkflowMeta::object(
            ObjectKey::new("parent/run"),
            ParentWorkflow::Run,
            "parent run",
        ))
    }

    async fn handle(
        &self,
        _request: Self::Request,
        context: WorkflowContext,
    ) -> RuntimeResult<LeafReply> {
        self.router.call(&context, LeafRequest::Run("leaf/a")).await
    }
}

async fn leaf_stats(router: &Router) -> (usize, usize) {
    let LeafReply::Stats { started, stable } = router
        .call_root("leaf stats", LeafRequest::Stats)
        .await
        .unwrap()
    else {
        panic!("expected leaf stats")
    };
    (started, stable)
}

#[tokio::test]
async fn duplicate_object_intents_join_one_runtime_task() {
    let router = Router::new();
    let leaf = spawn_service(
        Arc::new(LeafService::default()),
        &router,
        Default::default(),
    );

    let first = router.call_root("first", LeafRequest::Run("leaf/a"));
    let second = router.call_root("second", LeafRequest::Run("leaf/a"));
    let (first, second) = tokio::join!(first, second);

    assert_eq!(first.unwrap(), LeafReply::Done);
    assert_eq!(second.unwrap(), LeafReply::Done);
    assert_eq!(leaf_stats(&router).await, (1, 0));
    leaf.shutdown().await.unwrap();
}

#[tokio::test]
async fn dropping_a_root_call_propagates_cancellation_to_the_leaf() {
    let router = Router::new();
    let leaf = spawn_service(
        Arc::new(LeafService::default()),
        &router,
        Default::default(),
    );
    let parent = spawn_service(
        Arc::new(ParentService {
            router: router.clone(),
        }),
        &router,
        Default::default(),
    );
    let caller_router = router.clone();
    let caller = tokio::spawn(async move {
        caller_router
            .call_root("abandoned parent", ParentRequest::Run)
            .await
    });

    tokio::time::sleep(Duration::from_millis(10)).await;
    caller.abort();
    let _ = caller.await;
    tokio::time::sleep(Duration::from_millis(20)).await;

    assert_eq!(leaf_stats(&router).await, (1, 1));
    assert_eq!(parent.observer.snapshot().active_tasks, 0);
    parent.shutdown().await.unwrap();
    leaf.shutdown().await.unwrap();
}

#[tokio::test]
async fn force_abort_cancels_owned_calls_before_aborting_the_service_task() {
    let router = Router::new();
    let leaf = spawn_service(
        Arc::new(LeafService::default()),
        &router,
        Default::default(),
    );
    let parent = spawn_service(
        Arc::new(ParentService {
            router: router.clone(),
        }),
        &router,
        Default::default(),
    );
    let caller_router = router.clone();
    let caller = tokio::spawn(async move {
        caller_router
            .call_root("unloaded parent", ParentRequest::Run)
            .await
    });

    tokio::time::sleep(Duration::from_millis(10)).await;
    parent.force_abort();
    assert!(matches!(
        caller.await.unwrap(),
        Err(RuntimeError::ChannelClosed(_))
    ));
    tokio::time::sleep(Duration::from_millis(20)).await;

    assert_eq!(leaf_stats(&router).await, (1, 1));
    drop(parent);
    leaf.shutdown().await.unwrap();
}
