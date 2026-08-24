use std::{
    future::pending,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use mdc_runtime::{
    HandlerRegistry, MessageContext, OperationId, PoolServices, RuntimeError, ServiceContext,
    ServiceLifecycle, ServiceShutdown, TaskKey, TaskOutcome, TaskVisibility, TraceContext,
    define_messages, register_handlers, request_channel,
};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum TestService {
    Worker,
}

#[derive(Debug)]
struct Start {
    marker: DropMarker,
    started: mdc_runtime::Reply<()>,
}

#[derive(Debug)]
struct Finished {
    outcome: TaskOutcome<()>,
}

define_messages! {
    enum TestMessage => TestMessageKind {
        Start(Start),
        Finished(Finished)
    }
}

#[derive(Debug)]
struct DropMarker(Arc<AtomicBool>);

impl Drop for DropMarker {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

fn start(
    service: &mut ServiceContext<TestService, TestMessage, ()>,
    request: Start,
) -> Result<(), RuntimeError> {
    service.run(
        TaskKey::new("pending"),
        "pending workflow",
        TaskVisibility::Internal,
        move |_| async move {
            let _marker = request.marker;
            let _ = request.started.send(());
            pending::<()>().await;
            Ok(())
        },
        move |outcome| TestMessage::Finished(Finished { outcome }),
    );
    Ok(())
}

fn finished(
    _service: &mut ServiceContext<TestService, TestMessage, ()>,
    event: Finished,
) -> Result<(), RuntimeError> {
    let _ = event.outcome;
    Ok(())
}

fn context() -> MessageContext {
    MessageContext::new(OperationId(1), TraceContext::root(1))
}

#[tokio::test]
async fn control_channel_is_prioritized_and_lifecycle_rejects_new_business() {
    let mut registry = HandlerRegistry::new();
    register_handlers!(registry, { Start => start, Finished => finished }).unwrap();
    let mut services = PoolServices::new();
    let worker = services
        .spawn(TestService::Worker, (), registry, 8)
        .await
        .unwrap();

    worker.control.pause().await.unwrap();
    assert_eq!(
        worker.observer.snapshot().lifecycle,
        ServiceLifecycle::Paused
    );
    let (started, _ticket) = request_channel();
    let error = worker
        .command
        .send_payload(
            Start {
                marker: DropMarker(Arc::new(AtomicBool::new(false))),
                started,
            },
            context(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, RuntimeError::ServiceUnavailable(_)));

    worker.control.resume().await.unwrap();
    worker.control.drain().await.unwrap();
    assert_eq!(
        worker.observer.snapshot().lifecycle,
        ServiceLifecycle::Paused
    );
    worker
        .control
        .shutdown(ServiceShutdown::Immediate)
        .await
        .unwrap();
    assert_eq!(
        worker.observer.snapshot().lifecycle,
        ServiceLifecycle::Stopped
    );
}

#[tokio::test]
async fn dropping_the_service_container_drops_every_managed_future() {
    let mut registry = HandlerRegistry::new();
    register_handlers!(registry, { Start => start, Finished => finished }).unwrap();
    let mut services = PoolServices::new();
    let worker = services
        .spawn(TestService::Worker, (), registry, 8)
        .await
        .unwrap();
    let dropped = Arc::new(AtomicBool::new(false));
    let (started, ticket) = request_channel();
    worker
        .command
        .send_payload(
            Start {
                marker: DropMarker(dropped.clone()),
                started,
            },
            context(),
        )
        .await
        .unwrap();
    ticket.await.unwrap();

    drop(services);
    tokio::time::timeout(Duration::from_secs(1), async {
        while !dropped.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("managed future leaked after ServiceEntry was dropped");
}
