use std::{
    future::pending,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use mdc_runtime::{
    CancelReason, ConflictPolicy, RequestContext, Service, ServiceGroup, ServiceLifecycle,
    ServiceTaskManager, ShutdownMode, Submission, TaskExit, TaskKey, TaskMeta,
};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum Kind {
    Test,
}

enum Request {
    Query,
    Work,
    Never {
        marker: DropMarker,
        started: tokio::sync::oneshot::Sender<()>,
    },
}

#[derive(Debug, Eq, PartialEq)]
enum Response {
    Value(u64),
    Finished,
}

#[derive(Debug)]
struct Error;

struct DropMarker(Arc<AtomicBool>);

impl Drop for DropMarker {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

struct TestService {
    tasks: ServiceTaskManager<Kind>,
}

impl TestService {
    fn new() -> Self {
        Self {
            tasks: ServiceTaskManager::new(Kind::Test),
        }
    }

    async fn work_workflow(self: Arc<Self>, context: RequestContext) -> Result<Response, Error> {
        let task = self
            .create_new_task(
                &context,
                TaskMeta::new(TaskKey::new("work"), "test work"),
                ConflictPolicy::Reject,
            )
            .await
            .map_err(|_| Error)?;
        task.cancelled().await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        Ok(Response::Finished)
    }

    async fn never_workflow(
        self: Arc<Self>,
        marker: DropMarker,
        started: tokio::sync::oneshot::Sender<()>,
    ) -> Result<Response, Error> {
        let _marker = marker;
        let _ = started.send(());
        pending::<()>().await;
        Ok(Response::Finished)
    }
}

impl Service for TestService {
    type Key = Kind;
    type Request = Request;
    type Response = Response;
    type Error = Error;

    fn task_manager(&self) -> &ServiceTaskManager<Kind> {
        &self.tasks
    }

    async fn handle(
        self: Arc<Self>,
        request: Request,
        context: RequestContext,
    ) -> Result<Response, Error> {
        match request {
            Request::Query => Ok(Response::Value(7)),
            Request::Work => self.work_workflow(context).await,
            Request::Never { marker, started } => self.never_workflow(marker, started).await,
        }
    }
}

#[tokio::test]
async fn handler_future_can_optionally_create_a_managed_task() {
    let mut services = ServiceGroup::new();
    let service = services
        .spawn(Kind::Test, Arc::new(TestService::new()), 8)
        .await
        .unwrap();
    let mut events = service.observer.task_events();

    assert_eq!(
        service.client.call(Request::Query).await.unwrap(),
        Response::Value(7)
    );
    assert!(events.try_recv().is_err());
    assert!(service.observer.task_snapshots().is_empty());

    let Submission::Task(ticket) = service.client.submit(Request::Work).await.unwrap() else {
        panic!("work must create a managed task");
    };
    let exit = ticket
        .cancel_and_wait(CancelReason::requested("test"))
        .await
        .unwrap();
    assert!(matches!(exit, TaskExit::Cancelled(_)));
    assert!(service.observer.task_snapshots().is_empty());

    services
        .shutdown_all(ShutdownMode::Immediate)
        .await
        .unwrap();
}

#[tokio::test]
async fn lifecycle_control_rejects_new_requests_while_paused() {
    let mut services = ServiceGroup::new();
    let service = services
        .spawn(Kind::Test, Arc::new(TestService::new()), 8)
        .await
        .unwrap();

    service.control.pause().await.unwrap();
    assert_eq!(
        service.observer.snapshot().lifecycle,
        ServiceLifecycle::Paused
    );
    assert!(service.client.submit(Request::Query).await.is_err());

    service.control.resume().await.unwrap();
    assert_eq!(
        service.client.call(Request::Query).await.unwrap(),
        Response::Value(7)
    );
    service.control.drain().await.unwrap();
    assert_eq!(
        service.observer.snapshot().lifecycle,
        ServiceLifecycle::Paused
    );
    services
        .shutdown_all(ShutdownMode::Immediate)
        .await
        .unwrap();
}

#[tokio::test]
async fn dropping_the_service_group_drops_every_handler_future() {
    let mut services = ServiceGroup::new();
    let service = services
        .spawn(Kind::Test, Arc::new(TestService::new()), 8)
        .await
        .unwrap();
    let dropped = Arc::new(AtomicBool::new(false));
    let (started, ready) = tokio::sync::oneshot::channel();
    service
        .client
        .send(Request::Never {
            marker: DropMarker(dropped.clone()),
            started,
        })
        .await
        .unwrap();
    ready.await.unwrap();
    assert_eq!(service.observer.snapshot().inflight_requests, 1);
    assert!(service.observer.task_snapshots().is_empty());

    drop(services);
    tokio::time::timeout(Duration::from_secs(1), async {
        while !dropped.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("managed future leaked after ServiceGroup drop");
}
