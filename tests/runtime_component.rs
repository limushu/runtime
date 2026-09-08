use async_trait::async_trait;
use pool_control_plane::runtime::{
    ManagedService, OperationSpec, RequestContext, RuntimeEventKind, RuntimeEventSink,
    ServiceActivity, ServiceConfig, ServiceContext, ServiceLifecycle, ServiceMessage,
    ServiceRequest, ServiceRuntime, ServiceUnavailable, TaskMeta, TaskOutcome, TraceContext,
};
use std::{
    fmt,
    sync::{Arc, Mutex},
};
use tokio::sync::{Semaphore, oneshot};

#[derive(Debug, Clone, PartialEq, Eq)]
enum ProbeError {
    Stopped,
    Unavailable(ServiceUnavailable),
    Cancelled,
}

impl fmt::Display for ProbeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

enum ProbeMessage {
    Query(oneshot::Sender<Result<&'static str, ProbeError>>),
    Work(oneshot::Sender<Result<(), ProbeError>>),
    Crash(oneshot::Sender<Result<(), ProbeError>>),
}

impl ServiceMessage for ProbeMessage {
    type Error = ProbeError;

    fn stopped() -> Self::Error {
        ProbeError::Stopped
    }

    fn unavailable(reason: ServiceUnavailable) -> Self::Error {
        ProbeError::Unavailable(reason)
    }

    fn reject(self, reason: ServiceUnavailable) {
        match self {
            Self::Query(reply) => {
                let _ = reply.send(Err(ProbeError::Unavailable(reason)));
            }
            Self::Work(reply) => {
                let _ = reply.send(Err(ProbeError::Unavailable(reason)));
            }
            Self::Crash(reply) => {
                let _ = reply.send(Err(ProbeError::Unavailable(reason)));
            }
        }
    }
}

struct Query;
struct Work;
struct Crash;

impl ServiceRequest<ProbeMessage> for Query {
    type Response = &'static str;

    fn into_message(
        self,
        reply: oneshot::Sender<Result<Self::Response, ProbeError>>,
    ) -> ProbeMessage {
        ProbeMessage::Query(reply)
    }
}

impl ServiceRequest<ProbeMessage> for Work {
    type Response = ();

    fn operation(&self) -> Option<OperationSpec> {
        Some(
            OperationSpec::new("probe", "work", "object/1", "controlled test work")
                .with_trace(TraceContext::new("probe-trace")),
        )
    }

    fn into_message(
        self,
        reply: oneshot::Sender<Result<Self::Response, ProbeError>>,
    ) -> ProbeMessage {
        ProbeMessage::Work(reply)
    }
}

impl ServiceRequest<ProbeMessage> for Crash {
    type Response = ();

    fn operation(&self) -> Option<OperationSpec> {
        Some(OperationSpec::new(
            "probe",
            "crash",
            "object/1",
            "panic isolation test",
        ))
    }

    fn into_message(
        self,
        reply: oneshot::Sender<Result<Self::Response, ProbeError>>,
    ) -> ProbeMessage {
        ProbeMessage::Crash(reply)
    }
}

struct ProbeService {
    block_initialization: bool,
    init_started: Semaphore,
    init_release: Semaphore,
    work_started: Semaphore,
    work_release: Semaphore,
    activity: Mutex<Vec<ServiceActivity>>,
}

#[derive(Default)]
struct RecordingSink {
    events: Mutex<Vec<pool_control_plane::runtime::RuntimeEvent>>,
}

impl RuntimeEventSink for RecordingSink {
    fn record(&self, event: &pool_control_plane::runtime::RuntimeEvent) {
        self.events.lock().unwrap().push(event.clone());
    }
}

impl ProbeService {
    fn ready() -> Arc<Self> {
        Arc::new(Self {
            block_initialization: false,
            init_started: Semaphore::new(0),
            init_release: Semaphore::new(0),
            work_started: Semaphore::new(0),
            work_release: Semaphore::new(0),
            activity: Mutex::new(Vec::new()),
        })
    }

    fn blocked_initialization() -> Arc<Self> {
        Arc::new(Self {
            block_initialization: true,
            init_started: Semaphore::new(0),
            init_release: Semaphore::new(0),
            work_started: Semaphore::new(0),
            work_release: Semaphore::new(0),
            activity: Mutex::new(Vec::new()),
        })
    }

    async fn wait_init_started(&self) {
        self.init_started.acquire().await.unwrap().forget();
    }

    fn release_init(&self) {
        self.init_release.add_permits(1);
    }

    async fn wait_work_started(&self) {
        self.work_started.acquire().await.unwrap().forget();
    }

    fn release_work(&self) {
        self.work_release.add_permits(1);
    }
}

#[async_trait]
impl ManagedService for ProbeService {
    type Message = ProbeMessage;

    async fn initialize(&self, context: ServiceContext) -> Result<(), ProbeError> {
        self.init_started.add_permits(1);
        if self.block_initialization {
            tokio::select! {
                _ = context.cancellation().cancelled() => return Err(ProbeError::Cancelled),
                permit = self.init_release.acquire() => permit.unwrap().forget(),
            }
        }
        Ok(())
    }

    async fn handle(
        self: Arc<Self>,
        message: Self::Message,
        context: RequestContext,
    ) -> Result<(), ProbeError> {
        match message {
            ProbeMessage::Query(reply) => {
                let _ = reply.send(Ok("pong"));
                Ok(())
            }
            ProbeMessage::Work(reply) => {
                let task = context.start_task(
                    TaskMeta::new("object/1", "probe-work", "controlled probe task"),
                    context.cancellation().child_token(),
                );
                task.progress(10);
                task.milestone("waiting for test release");
                task.blocked_on("test gate");
                self.work_started.add_permits(1);

                let result = tokio::select! {
                    _ = task.cancellation().cancelled() => Err(ProbeError::Cancelled),
                    permit = self.work_release.acquire() => {
                        permit.unwrap().forget();
                        Ok(())
                    }
                };
                task.unblocked();
                let outcome = match &result {
                    Ok(()) => TaskOutcome::Completed,
                    Err(ProbeError::Cancelled) => TaskOutcome::Cancelled,
                    Err(error) => TaskOutcome::Failed(error.to_string()),
                };
                task.finish(outcome);
                let _ = reply.send(result.clone());
                result
            }
            ProbeMessage::Crash(_reply) => panic!("probe handler crashed"),
        }
    }

    fn activity_changed(&self, activity: ServiceActivity) {
        self.activity.lock().unwrap().push(activity);
    }
}

fn spawn(service: Arc<ProbeService>) -> pool_control_plane::runtime::ServiceInstance<ProbeMessage> {
    let mut config = ServiceConfig::new("probe-service", "probe");
    config.max_in_flight_requests = 4;
    ServiceRuntime::spawn_arc(service, config)
}

async fn wait_for_lifecycle(
    observer: &mut pool_control_plane::runtime::ServiceObserver,
    lifecycle: ServiceLifecycle,
) {
    while observer.snapshot().lifecycle != lifecycle {
        observer.changed().await.unwrap();
    }
}

#[tokio::test]
async fn initialization_and_running_are_observable() {
    let service = ProbeService::blocked_initialization();
    let running = spawn(service.clone());
    let mut observer = running.observer.clone();

    service.wait_init_started().await;
    assert_eq!(
        observer.snapshot().lifecycle,
        ServiceLifecycle::Initializing
    );

    service.release_init();
    wait_for_lifecycle(&mut observer, ServiceLifecycle::Running).await;
    assert_eq!(running.client.call(Query).await.unwrap(), "pong");

    running.control.drain().await.unwrap();
    running.task.await.unwrap();
}

#[tokio::test]
async fn requests_wait_for_initialization_and_stop_can_cancel_initialization() {
    let service = ProbeService::blocked_initialization();
    let running = spawn(service.clone());
    service.wait_init_started().await;

    let client = running.client.clone();
    let query = tokio::spawn(async move { client.call(Query).await });
    let mut observer = running.observer.clone();
    observer
        .wait_for(|snapshot| snapshot.queued_requests == 1)
        .await
        .unwrap();
    assert!(!query.is_finished());

    running.control.stop().await.unwrap();
    assert_eq!(
        query.await.unwrap(),
        Err(ProbeError::Unavailable(ServiceUnavailable::Stopped))
    );
    let snapshot = running.observer.snapshot();
    assert_eq!(snapshot.lifecycle, ServiceLifecycle::Stopped);
    assert_eq!(snapshot.queued_requests, 0);
    assert_eq!(snapshot.rejected_requests, 1);
    running.task.await.unwrap();
}

#[tokio::test]
async fn pause_rejects_new_business_and_resume_reopens_admission() {
    let running = spawn(ProbeService::ready());
    let mut observer = running.observer.clone();
    wait_for_lifecycle(&mut observer, ServiceLifecycle::Running).await;

    running.control.pause().await.unwrap();
    assert_eq!(
        running.client.call(Query).await,
        Err(ProbeError::Unavailable(ServiceUnavailable::Paused))
    );
    assert_eq!(observer.snapshot().rejected_requests, 1);

    running.control.resume().await.unwrap();
    assert_eq!(running.client.call(Query).await.unwrap(), "pong");

    running.control.drain().await.unwrap();
    running.task.await.unwrap();
}

#[tokio::test]
async fn control_preempts_a_full_business_window_and_queue_depth_is_observable() {
    let service = ProbeService::ready();
    let mut config = ServiceConfig::new("probe-service", "probe");
    config.max_in_flight_requests = 1;
    let running = ServiceRuntime::spawn_arc(service.clone(), config);
    let mut observer = running.observer.clone();
    wait_for_lifecycle(&mut observer, ServiceLifecycle::Running).await;

    let first_client = running.client.clone();
    let first = tokio::spawn(async move { first_client.call(Work).await });
    service.wait_work_started().await;

    let second_client = running.client.clone();
    let second = tokio::spawn(async move { second_client.call(Work).await });
    observer
        .wait_for(|snapshot| snapshot.queued_requests == 1)
        .await
        .unwrap();

    running.control.pause().await.unwrap();
    assert_eq!(observer.snapshot().lifecycle, ServiceLifecycle::Paused);
    assert!(!first.is_finished());

    service.release_work();
    first.await.unwrap().unwrap();
    assert_eq!(
        second.await.unwrap(),
        Err(ProbeError::Unavailable(ServiceUnavailable::Paused))
    );
    observer
        .wait_for(|snapshot| snapshot.queued_requests == 0)
        .await
        .unwrap();
    assert_eq!(observer.snapshot().accepted_requests, 1);
    assert_eq!(observer.snapshot().rejected_requests, 1);

    running.control.resume().await.unwrap();
    running.control.drain().await.unwrap();
    running.task.await.unwrap();
}

#[tokio::test]
async fn drain_waits_for_accepted_work_but_rejects_new_work() {
    let service = ProbeService::ready();
    let running = spawn(service.clone());
    let mut observer = running.observer.clone();
    wait_for_lifecycle(&mut observer, ServiceLifecycle::Running).await;

    let client = running.client.clone();
    let work = tokio::spawn(async move { client.call(Work).await });
    service.wait_work_started().await;
    assert_eq!(observer.snapshot().activity, ServiceActivity::Busy);
    assert_eq!(observer.snapshot().active_tasks.len(), 1);

    let control = running.control.clone();
    let drain = tokio::spawn(async move { control.drain().await });
    wait_for_lifecycle(&mut observer, ServiceLifecycle::Draining).await;
    assert_eq!(
        running.client.call(Query).await,
        Err(ProbeError::Unavailable(ServiceUnavailable::Draining))
    );
    assert!(!drain.is_finished());

    service.release_work();
    work.await.unwrap().unwrap();
    drain.await.unwrap().unwrap();
    assert_eq!(observer.snapshot().lifecycle, ServiceLifecycle::Stopped);
    assert_eq!(observer.snapshot().activity, ServiceActivity::Idle);
    assert_eq!(observer.snapshot().in_flight_requests, 0);
    assert!(observer.snapshot().active_tasks.is_empty());
    running.task.await.unwrap();
}

#[tokio::test]
async fn stop_and_precise_task_cancel_are_cooperative_and_audited() {
    let service = ProbeService::ready();
    let running = spawn(service.clone());
    let mut observer = running.observer.clone();
    wait_for_lifecycle(&mut observer, ServiceLifecycle::Running).await;

    let client = running.client.clone();
    let work = tokio::spawn(async move { client.call(Work).await });
    service.wait_work_started().await;
    let task_snapshot = observer.snapshot().active_tasks[0].clone();
    let task = task_snapshot.id;
    assert_eq!(task_snapshot.trace_id, "probe-trace");
    assert_eq!(task_snapshot.progress, Some(10));
    assert_eq!(
        task_snapshot.milestone.as_deref(),
        Some("waiting for test release")
    );
    assert_eq!(task_snapshot.blocked_on.as_deref(), Some("test gate"));
    running
        .control
        .cancel_task(task, "operator cancelled probe")
        .await
        .unwrap();
    assert_eq!(work.await.unwrap(), Err(ProbeError::Cancelled));
    while observer.snapshot().activity != ServiceActivity::Idle {
        observer.changed().await.unwrap();
    }

    let history = observer.history();
    assert!(history.iter().any(|event| matches!(
        &event.kind,
        RuntimeEventKind::TaskCancelRequested { task: id, cause }
            if *id == task && cause == "operator cancelled probe"
    )));
    assert!(history.iter().any(|event| matches!(
        &event.kind,
        RuntimeEventKind::TaskFinished {
            task: id,
            outcome: TaskOutcome::Cancelled,
            ..
        } if *id == task
    )));

    running.control.stop().await.unwrap();
    running.task.await.unwrap();
}

#[tokio::test]
async fn query_has_no_task_and_activity_edges_are_reported() {
    let service = ProbeService::ready();
    let running = spawn(service.clone());
    let mut observer = running.observer.clone();
    wait_for_lifecycle(&mut observer, ServiceLifecycle::Running).await;

    assert_eq!(running.client.call(Query).await.unwrap(), "pong");
    while observer.snapshot().activity != ServiceActivity::Idle
        || observer.snapshot().completed_requests == 0
    {
        observer.changed().await.unwrap();
    }
    assert!(observer.snapshot().active_tasks.is_empty());
    assert_eq!(observer.snapshot().activity, ServiceActivity::Idle);
    assert_eq!(observer.snapshot().in_flight_requests, 0);
    assert!(
        !observer
            .history()
            .iter()
            .any(|event| matches!(event.kind, RuntimeEventKind::TaskStarted(_)))
    );
    assert_eq!(
        service.activity.lock().unwrap().as_slice(),
        [ServiceActivity::Busy, ServiceActivity::Idle]
    );

    running.control.drain().await.unwrap();
    running.task.await.unwrap();
}

#[tokio::test]
async fn force_abort_drops_all_futures_and_marks_tasks_aborted() {
    let service = ProbeService::ready();
    let running = spawn(service.clone());
    let mut observer = running.observer.clone();
    wait_for_lifecycle(&mut observer, ServiceLifecycle::Running).await;

    let client = running.client.clone();
    let work = tokio::spawn(async move { client.call(Work).await });
    service.wait_work_started().await;
    let task = observer.snapshot().active_tasks[0].id;

    running.task.abort();
    assert!(running.task.await.unwrap_err().is_cancelled());
    assert_eq!(observer.snapshot().lifecycle, ServiceLifecycle::Stopped);
    assert_eq!(observer.snapshot().activity, ServiceActivity::Idle);
    assert_eq!(observer.snapshot().in_flight_requests, 0);
    assert!(observer.snapshot().active_tasks.is_empty());
    assert!(observer.history().iter().any(|event| matches!(
        &event.kind,
        RuntimeEventKind::TaskFinished {
            task: id,
            outcome: TaskOutcome::Aborted,
            ..
        } if *id == task
    )));
    assert_eq!(work.await.unwrap(), Err(ProbeError::Stopped));
}

#[tokio::test]
async fn dropping_the_root_owner_cannot_leak_the_service_task() {
    let service = ProbeService::ready();
    let running = spawn(service.clone());
    let mut observer = running.observer.clone();
    let client = running.client.clone();
    wait_for_lifecycle(&mut observer, ServiceLifecycle::Running).await;

    let work_client = client.clone();
    let work = tokio::spawn(async move { work_client.call(Work).await });
    service.wait_work_started().await;

    drop(running.task);
    observer
        .wait_for(|snapshot| snapshot.lifecycle == ServiceLifecycle::Stopped)
        .await
        .unwrap();

    assert!(observer.snapshot().active_tasks.is_empty());
    assert_eq!(observer.snapshot().activity, ServiceActivity::Idle);
    assert_eq!(observer.snapshot().in_flight_requests, 0);
    assert_eq!(work.await.unwrap(), Err(ProbeError::Stopped));
    assert_eq!(
        client.call(Query).await,
        Err(ProbeError::Unavailable(ServiceUnavailable::Stopped))
    );
}

#[tokio::test]
async fn a_handler_panic_is_reported_as_service_failure() {
    let running = spawn(ProbeService::ready());
    let mut observer = running.observer.clone();
    wait_for_lifecycle(&mut observer, ServiceLifecycle::Running).await;

    assert_eq!(running.client.call(Crash).await, Err(ProbeError::Stopped));
    observer
        .wait_for(|snapshot| snapshot.lifecycle == ServiceLifecycle::Failed)
        .await
        .unwrap();

    let snapshot = observer.snapshot();
    assert_eq!(snapshot.activity, ServiceActivity::Idle);
    assert_eq!(snapshot.in_flight_requests, 0);
    assert_eq!(snapshot.queued_requests, 0);
    assert!(
        snapshot
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("probe handler crashed"))
    );
    running.task.await.unwrap();
}

#[tokio::test]
async fn structured_events_can_be_exported_without_parsing_logs() {
    let sink = Arc::new(RecordingSink::default());
    let mut config = ServiceConfig::new("probe-service", "probe");
    config.event_sink = Some(sink.clone());
    let running = ServiceRuntime::spawn_arc(ProbeService::ready(), config);
    let mut observer = running.observer.clone();
    wait_for_lifecycle(&mut observer, ServiceLifecycle::Running).await;

    assert_eq!(running.client.call(Query).await.unwrap(), "pong");
    observer
        .wait_for(|snapshot| snapshot.completed_requests == 1)
        .await
        .unwrap();
    running.control.drain().await.unwrap();
    running.task.await.unwrap();

    let events = sink.events.lock().unwrap();
    assert!(
        events
            .iter()
            .any(|event| matches!(event.kind, RuntimeEventKind::RequestFinished { .. }))
    );
    assert!(events.iter().any(|event| matches!(
        event.kind,
        RuntimeEventKind::LifecycleChanged {
            to: ServiceLifecycle::Stopped,
            ..
        }
    )));
}
