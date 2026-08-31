use async_trait::async_trait;
use control_runtime::{
    spawn_service, Admission, ObjectActivity, ObjectKey, RequestRoute, RuntimeError, RuntimeResult,
    Service, ServiceClient, ServiceId, ServiceRequest, StateCell, WorkflowContext, WorkflowMeta,
};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone)]
enum LeafRequest {
    Run(&'static str),
    Panic,
    Stats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeafReply {
    Done,
    Stats { started: usize, stable: usize },
}

impl ServiceRequest for LeafRequest {
    type Response = LeafReply;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeafWorkflow {
    Run,
    Panic,
}

#[derive(Debug, Default)]
struct LeafStats {
    started: usize,
    stable: usize,
}

#[derive(Debug, Default)]
struct LeafWorker {
    stats: StateCell<LeafStats>,
}

#[async_trait]
impl Service for LeafWorker {
    type Request = LeafRequest;
    type WorkflowKind = LeafWorkflow;

    fn id(&self) -> ServiceId {
        ServiceId::new("contract.leaf")
    }

    fn route(&self, request: &Self::Request) -> RequestRoute<Self::WorkflowKind> {
        match request {
            LeafRequest::Run(key) => RequestRoute::Workflow(WorkflowMeta::object(
                ObjectKey::new(*key),
                LeafWorkflow::Run,
                format!("run {key}"),
            )),
            LeafRequest::Panic => RequestRoute::Workflow(WorkflowMeta::object(
                ObjectKey::new("leaf/panic"),
                LeafWorkflow::Panic,
                "panic",
            )),
            LeafRequest::Stats => RequestRoute::Untracked,
        }
    }

    fn admit(
        &self,
        _context: &WorkflowContext,
        _request: &Self::Request,
        activity: &ObjectActivity<Self::WorkflowKind>,
    ) -> RuntimeResult<Admission<LeafReply>> {
        Ok(if activity.is_idle() {
            Admission::Start
        } else {
            Admission::Join
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
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        self.stats.update(|stats| stats.stable += 1);
                        Err(RuntimeError::Cancelled)
                    }
                    _ = tokio::time::sleep(Duration::from_millis(50)) => Ok(LeafReply::Done),
                }
            }
            LeafRequest::Panic => panic!("domain workflow panic"),
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParentWorkflow {
    Run,
}

struct ParentWorker {
    leaf: ServiceClient<LeafRequest>,
}

#[async_trait]
impl Service for ParentWorker {
    type Request = ParentRequest;
    type WorkflowKind = ParentWorkflow;

    fn id(&self) -> ServiceId {
        ServiceId::new("contract.parent")
    }

    fn route(&self, _request: &Self::Request) -> RequestRoute<Self::WorkflowKind> {
        RequestRoute::Workflow(WorkflowMeta::object(
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
        self.leaf
            .call(&context, "run leaf/a", LeafRequest::Run("leaf/a"))
            .await
    }
}

async fn leaf_stats(client: &ServiceClient<LeafRequest>) -> (usize, usize) {
    let LeafReply::Stats { started, stable } = client
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
    let (client, host) = spawn_service(Arc::new(LeafWorker::default()), Default::default());
    let first = client.call_root("first", LeafRequest::Run("leaf/a"));
    let second = client.call_root("second", LeafRequest::Run("leaf/a"));
    let (first, second) = tokio::join!(first, second);
    assert_eq!(first.unwrap(), LeafReply::Done);
    assert_eq!(second.unwrap(), LeafReply::Done);
    assert_eq!(leaf_stats(&client).await, (1, 0));
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn dropping_one_joined_caller_keeps_the_shared_workflow() {
    let (client, host) = spawn_service(Arc::new(LeafWorker::default()), Default::default());
    let first_client = client.clone();
    let first = tokio::spawn(async move {
        first_client
            .call_root("first", LeafRequest::Run("leaf/shared"))
            .await
    });
    tokio::time::sleep(Duration::from_millis(5)).await;
    let second_client = client.clone();
    let second = tokio::spawn(async move {
        second_client
            .call_root("second", LeafRequest::Run("leaf/shared"))
            .await
    });
    tokio::time::sleep(Duration::from_millis(5)).await;
    first.abort();
    let _ = first.await;
    assert_eq!(second.await.unwrap().unwrap(), LeafReply::Done);
    assert_eq!(leaf_stats(&client).await, (1, 0));
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_domain_panic_fails_only_its_workflow() {
    let (client, host) = spawn_service(Arc::new(LeafWorker::default()), Default::default());
    assert!(matches!(
        client.call_root("panic", LeafRequest::Panic).await,
        Err(RuntimeError::WorkflowPanicked(message)) if message == "domain workflow panic"
    ));
    assert_eq!(leaf_stats(&client).await, (0, 0));
    assert_eq!(host.observer.snapshot().active_tasks, 0);
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn dropping_a_root_call_propagates_cancellation_to_the_leaf() {
    let (leaf_client, leaf_host) =
        spawn_service(Arc::new(LeafWorker::default()), Default::default());
    let (parent_client, parent_host) = spawn_service(
        Arc::new(ParentWorker {
            leaf: leaf_client.clone(),
        }),
        Default::default(),
    );
    let caller = tokio::spawn(async move {
        parent_client
            .call_root("abandoned parent", ParentRequest::Run)
            .await
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    caller.abort();
    let _ = caller.await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(leaf_stats(&leaf_client).await, (1, 1));
    assert_eq!(parent_host.observer.snapshot().active_tasks, 0);
    parent_host.shutdown().await.unwrap();
    leaf_host.shutdown().await.unwrap();
}
