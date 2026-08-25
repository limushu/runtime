use std::{
    future::pending,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use mdc_runtime::{
    CancelReason, HandleResult, RequestContext, Service, ServiceGroup, ServiceLifecycle,
    ShutdownMode, Submission, TaskExit, TaskKey, TaskMeta, TaskSpec,
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

struct TestService;

impl TestService {
    fn query(&self) -> HandleResult<Response, Error> {
        HandleResult::ok(Response::Value(7))
    }

    fn work_workflow(self: Arc<Self>) -> HandleResult<Response, Error> {
        HandleResult::task(TaskSpec::new(
            TaskMeta::new(TaskKey::new("work"), "test work"),
            move |task| async move {
                task.cancelled().await;
                tokio::time::sleep(Duration::from_millis(20)).await;
                Ok(Response::Finished)
            },
        ))
    }

    fn never_workflow(
        self: Arc<Self>,
        marker: DropMarker,
        started: tokio::sync::oneshot::Sender<()>,
    ) -> HandleResult<Response, Error> {
        HandleResult::task(TaskSpec::new(
            TaskMeta::new(TaskKey::new("never"), "never completes"),
            move |_| async move {
                let _marker = marker;
                let _ = started.send(());
                pending::<()>().await;
                Ok(Response::Finished)
            },
        ))
    }
}

impl Service for TestService {
    type Request = Request;
    type Response = Response;
    type Error = Error;

    fn handle(
        self: Arc<Self>,
        request: Request,
        _context: RequestContext,
    ) -> HandleResult<Response, Error> {
        match request {
            Request::Query => self.query(),
            Request::Work => self.work_workflow(),
            Request::Never { marker, started } => self.never_workflow(marker, started),
        }
    }
}

#[tokio::test]
async fn service_method_decides_between_an_immediate_reply_and_a_managed_task() {
    let mut services = ServiceGroup::new();
    let service = services
        .spawn(Kind::Test, Arc::new(TestService), 8)
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
        .spawn(Kind::Test, Arc::new(TestService), 8)
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
async fn dropping_the_service_group_drops_every_managed_future() {
    let mut services = ServiceGroup::new();
    let service = services
        .spawn(Kind::Test, Arc::new(TestService), 8)
        .await
        .unwrap();
    let dropped = Arc::new(AtomicBool::new(false));
    let (started, ready) = tokio::sync::oneshot::channel();
    let _submission = service
        .client
        .submit(Request::Never {
            marker: DropMarker(dropped.clone()),
            started,
        })
        .await
        .unwrap();
    ready.await.unwrap();

    drop(services);
    tokio::time::timeout(Duration::from_secs(1), async {
        while !dropped.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("managed future leaked after ServiceGroup drop");
}
