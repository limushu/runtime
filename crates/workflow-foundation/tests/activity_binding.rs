#[allow(dead_code)]
mod support;
use async_trait::async_trait;
use std::{sync::Arc, time::Duration};
use support::*;
use tokio::sync::Semaphore;
use workflow_foundation::*;

struct Flow<A>(A);
#[async_trait]
impl<A: Activity + 'static> Workflow for Flow<A>
where
    A::Output: 'static,
{
    type Output = A::Output;
    const NAME: &'static str = "binding";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<Self::Output> {
        ctx.run_activity(self.0).await
    }
}

struct Invoke {
    client: ServiceClient<Probe>,
    work: Option<Work>,
}
impl Invoke {
    fn new(client: &ServiceClient<Probe>, work: Work) -> Self {
        Self {
            client: client.clone(),
            work: Some(work),
        }
    }
}
// An ordinary domain helper needs no framework parameter either.
async fn call_domain(client: &ServiceClient<Probe>, work: Work) -> Outcome<usize> {
    tokio::task::yield_now().await;
    client.send(work).await
}
#[async_trait]
impl Activity for Invoke {
    type Reply = usize;
    type Output = usize;
    fn name(&self) -> &'static str {
        "invoke"
    }
    async fn execute(&mut self) -> Outcome<usize> {
        let id = activity::id()?;
        let result = call_domain(&self.client, self.work.take().unwrap()).await;
        assert_eq!(activity::id()?, id);
        result
    }
    fn handle(&mut self, result: Outcome<usize>) -> Outcome<ActivityAction<usize>> {
        // The handler interprets results; it cannot start unmanaged effects.
        assert!(activity::id().is_err());
        result.map(ActivityAction::Continue)
    }
}

struct TwoSteps(ServiceClient<Probe>);
#[async_trait]
impl Workflow for TwoSteps {
    type Output = usize;
    const NAME: &'static str = "two.steps";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<usize> {
        ctx.run_activity(Invoke::new(&self.0, Work::write("first", "disk")))
            .await?;
        assert!(activity::id().is_err());
        assert!(matches!(
            self.0.send(Work::write("outside", "disk")).await,
            Err(Failure::Rejected(_))
        ));
        ctx.run_activity(Invoke::new(&self.0, Work::write("second", "disk")))
            .await
    }
}

#[tokio::test]
async fn binding_is_limited_to_execute_and_is_replaced_for_each_step() {
    let (mut runtime, client, mut events) = setup();
    assert!(matches!(
        client.send(Work::write("outside", "disk")).await,
        Err(Failure::Rejected(_))
    ));
    assert!(activity::id().is_err());
    assert!(activity::control().is_err());
    assert!(activity::report_progress(1, Some(1)).is_err());
    assert!(activity::sleep(Duration::ZERO).await.is_err());
    assert!(activity::wait_until_runnable().await.is_err());
    assert_eq!(client.query(()).await.unwrap(), (0, vec![]));
    let mut run = runtime.submit(TwoSteps(client.clone())).await.unwrap();
    assert_eq!(finish(&mut run).await, Ok(2));
    assert_eq!(next(&mut events).await, "first");
    assert_eq!(next(&mut events).await, "second");
    assert!(events.try_recv().is_err());
    let history = run.control.activities();
    assert_eq!(history.total, 4);
    let first = &history.entries[0];
    let second = &history.entries[2];
    assert_ne!(first.id, second.id);
    assert_eq!(history.entries[1].parent_id, Some(first.id));
    assert_eq!(history.entries[3].parent_id, Some(second.id));
    runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shared_client_keeps_task_identity_and_cancellation_separate() {
    let (mut runtime, client, mut events) = setup();
    let stop = Arc::new(Semaphore::new(0));
    let finish_second = Arc::new(Semaphore::new(0));
    let mut first = runtime
        .submit(Flow(Invoke::new(
            &client,
            Work::write("first", "disk-a")
                .gated(&stop)
                .behavior(Behavior::CancelThenSettle),
        )))
        .await
        .unwrap();
    assert_eq!(next(&mut events).await, "first");
    let mut second = runtime
        .submit(Flow(Invoke::new(
            &client,
            Work::write("second", "disk-b").gated(&finish_second),
        )))
        .await
        .unwrap();
    assert_eq!(next(&mut events).await, "second");
    first.control.cancel();
    assert_eq!(next(&mut events).await, "cancel-observed");
    assert!(!second.control.cancellation_requested());
    stop.add_permits(1);
    assert_eq!(finish(&mut first).await, Err(Failure::Cancelled));
    finish_second.add_permits(1);
    assert_eq!(finish(&mut second).await, Ok(1));
    let calls = client.query(()).await.unwrap().1;
    for (run, call) in [(&first, calls[0]), (&second, calls[1])] {
        let history = run.control.activities();
        assert_eq!(history.entries.len(), 2);
        assert_eq!(call.task_id, run.id());
        assert_eq!(history.entries[1].id, call);
        assert_eq!(history.entries[1].parent_id, Some(history.entries[0].id));
    }
    runtime.shutdown().await.unwrap();
}

struct Spawned(ServiceClient<Probe>);
#[async_trait]
impl Activity for Spawned {
    type Reply = usize;
    type Output = usize;
    fn name(&self) -> &'static str {
        "spawned"
    }
    async fn execute(&mut self) -> Outcome<usize> {
        let client = self.0.clone();
        let result = tokio::spawn(async move {
            assert!(activity::id().is_err());
            client.send(Work::write("detached", "disk")).await
        })
        .await
        .unwrap();
        assert!(matches!(result, Err(Failure::Rejected(_))));
        self.0.send(Work::write("owned", "disk")).await
    }
    fn handle(&mut self, result: Outcome<usize>) -> Outcome<ActivityAction<usize>> {
        result.map(ActivityAction::Continue)
    }
}
#[tokio::test]
async fn detached_tasks_cannot_inherit_the_execution_capability() {
    let (mut runtime, client, mut events) = setup();
    let mut run = runtime.submit(Flow(Spawned(client))).await.unwrap();
    assert_eq!(finish(&mut run).await, Ok(1));
    assert_eq!(next(&mut events).await, "owned");
    assert!(events.try_recv().is_err());
    assert_eq!(run.control.activities().total, 2);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn implicit_binding_still_rejects_another_pool_instance() {
    let (mut first, _, _) = setup();
    let (mut second, client, mut events) = setup(); // Same label, distinct instance.
    let mut run = first
        .submit(Flow(Invoke::new(&client, Work::write("foreign", "disk"))))
        .await
        .unwrap();
    assert_eq!(
        finish(&mut run).await,
        Err(Failure::Rejected(
            "service belongs to another Pool instance".into()
        ))
    );
    assert!(events.try_recv().is_err());
    assert_eq!(client.query(()).await.unwrap(), (0, vec![]));
    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
}

struct Joined {
    client: ServiceClient<Probe>,
    gate: Arc<Semaphore>,
}
#[async_trait]
impl Activity for Joined {
    type Reply = usize;
    type Output = usize;
    fn name(&self) -> &'static str {
        "joined"
    }
    async fn execute(&mut self) -> Outcome<usize> {
        let (first, second) = tokio::join!(
            self.client
                .send(Work::write("first", "disk").gated(&self.gate)),
            self.client.send(Work::write("second", "disk")),
        );
        Ok(first? + second?)
    }
    fn handle(&mut self, result: Outcome<usize>) -> Outcome<ActivityAction<usize>> {
        result.map(ActivityAction::Continue)
    }
}
#[tokio::test]
async fn joined_calls_are_driven_serially_with_the_same_logical_parent() {
    let (mut runtime, client, mut events) = setup();
    let gate = Arc::new(Semaphore::new(0));
    let mut run = runtime
        .submit(Flow(Joined {
            client,
            gate: gate.clone(),
        }))
        .await
        .unwrap();
    assert_eq!(next(&mut events).await, "first");
    assert!(events.try_recv().is_err());
    gate.add_permits(1);
    assert_eq!(finish(&mut run).await, Ok(3));
    assert_eq!(next(&mut events).await, "second");
    let history = run.control.activities();
    assert_eq!(history.total, 3);
    let parent = history.entries[0].id;
    for call in history.entries.iter().skip(1) {
        assert_eq!(call.parent_id, Some(parent));
        assert_eq!(call.state, ActivityState::Completed);
    }
    runtime.shutdown().await.unwrap();
}

struct AbandonQueued {
    client: ServiceClient<Probe>,
    gate: Arc<Semaphore>,
    enqueue: Arc<Semaphore>,
    queued: Arc<Semaphore>,
}
#[async_trait]
impl Activity for AbandonQueued {
    type Reply = usize;
    type Output = usize;
    fn name(&self) -> &'static str {
        "abandon.queued"
    }
    async fn execute(&mut self) -> Outcome<usize> {
        let abandoned = async {
            self.enqueue.acquire().await.unwrap().forget();
            {
                let call = self.client.send(Work::write("abandoned", "disk"));
                tokio::pin!(call);
                assert!(futures_util::poll!(&mut call).is_pending());
            }
            self.queued.add_permits(1);
        };
        let (first, ()) = tokio::join!(
            self.client
                .send(Work::write("first", "disk").gated(&self.gate)),
            abandoned,
        );
        first?;
        self.client.send(Work::write("last", "disk")).await
    }
    fn handle(&mut self, result: Outcome<usize>) -> Outcome<ActivityAction<usize>> {
        result.map(ActivityAction::Continue)
    }
}
#[tokio::test]
async fn abandoned_queued_calls_never_start() {
    let (mut runtime, client, mut events) = setup();
    let gate = Arc::new(Semaphore::new(0));
    let enqueue = Arc::new(Semaphore::new(0));
    let queued = Arc::new(Semaphore::new(0));
    let mut run = runtime
        .submit(Flow(AbandonQueued {
            client,
            gate: gate.clone(),
            enqueue: enqueue.clone(),
            queued: queued.clone(),
        }))
        .await
        .unwrap();
    assert_eq!(next(&mut events).await, "first");
    enqueue.add_permits(1);
    queued.acquire().await.unwrap().forget();
    gate.add_permits(1);
    assert_eq!(finish(&mut run).await, Ok(2));
    assert_eq!(next(&mut events).await, "last");
    assert!(events.try_recv().is_err());
    assert_eq!(run.control.activities().total, 3);
    runtime.shutdown().await.unwrap();
}
