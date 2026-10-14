use super::*;
use crate::runtime::activity::*;
use async_trait::async_trait;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use tokio::sync::{Semaphore, mpsc};

fn claim(id: &str) -> ObjectClaim {
    ObjectClaim::exclusive(ObjectKey::new("document", id))
}
fn manager() -> ServiceInstance<ProcessManager> {
    spawn_service(
        ProcessManager::new(ExecutionScope::default()),
        ServiceConfig::default(),
    )
}
async fn next(rx: &mut mpsc::UnboundedReceiver<&'static str>) -> &'static str {
    tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap()
}
async fn idle(instance: &ServiceInstance<ProcessManager>) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if instance
                .client
                .query(())
                .await
                .unwrap()
                .workflows
                .iter()
                .all(|w| w.result.is_some())
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
fn gated(
    name: &'static str,
    claims: Vec<ObjectClaim>,
    tx: mpsc::UnboundedSender<&'static str>,
    gate: Arc<Semaphore>,
) -> WorkflowTask {
    WorkflowTask::new(name, claims, move |_| async move {
        tx.send(name).unwrap();
        gate.acquire().await.unwrap().forget();
        Ok(())
    })
}

#[tokio::test]
async fn arbitrary_workflows_serialize_same_object_and_run_other_objects() {
    let instance = manager();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let gate = Arc::new(Semaphore::new(0));
    for (name, key) in [("first", "a"), ("second", "a"), ("independent", "b")] {
        instance
            .client
            .submit(ProcessCommand::Start(gated(
                name,
                vec![claim(key)],
                tx.clone(),
                gate.clone(),
            )))
            .await
            .unwrap();
    }
    assert_eq!(next(&mut rx).await, "first");
    assert_eq!(next(&mut rx).await, "independent");
    assert!(rx.try_recv().is_err());
    gate.add_permits(2);
    assert_eq!(next(&mut rx).await, "second");
    gate.add_permits(1);
    instance.control.drain().await.unwrap();
    instance.join().await.unwrap();
}

#[tokio::test]
async fn multiple_object_sets_are_atomic_and_conflicting_waiters_are_fifo() {
    let instance = manager();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let gate = Arc::new(Semaphore::new(0));
    for (name, keys) in [
        ("a", vec![claim("a")]),
        ("ab", vec![claim("b"), claim("a")]),
        ("b", vec![claim("b")]),
    ] {
        instance
            .client
            .submit(ProcessCommand::Start(gated(
                name,
                keys,
                tx.clone(),
                gate.clone(),
            )))
            .await
            .unwrap();
    }
    assert_eq!(next(&mut rx).await, "a");
    assert!(rx.try_recv().is_err());
    gate.add_permits(1);
    assert_eq!(next(&mut rx).await, "ab");
    gate.add_permits(1);
    assert_eq!(next(&mut rx).await, "b");
    gate.add_permits(1);
    instance.control.drain().await.unwrap();
}

#[tokio::test]
async fn shared_objects_overlap_but_collection_writer_waits() {
    let instance = manager();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let gate = Arc::new(Semaphore::new(0));
    for name in ["r1", "r2"] {
        instance
            .client
            .submit(ProcessCommand::Start(gated(
                name,
                vec![ObjectClaim::shared(ObjectKey::new("document", "a"))],
                tx.clone(),
                gate.clone(),
            )))
            .await
            .unwrap();
    }
    instance
        .client
        .submit(ProcessCommand::Start(gated(
            "all",
            vec![ObjectClaim::exclusive(ObjectKey::collection("document"))],
            tx,
            gate.clone(),
        )))
        .await
        .unwrap();
    assert_eq!(next(&mut rx).await, "r1");
    assert_eq!(next(&mut rx).await, "r2");
    assert!(rx.try_recv().is_err());
    gate.add_permits(2);
    assert_eq!(next(&mut rx).await, "all");
    gate.add_permits(1);
    instance.control.drain().await.unwrap();
}

#[tokio::test]
async fn cancelling_running_or_waiting_workflows_never_releases_early() {
    let instance = manager();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let gate = Arc::new(Semaphore::new(0));
    let first = instance
        .client
        .submit(ProcessCommand::Start(gated(
            "first",
            vec![claim("a")],
            tx.clone(),
            gate.clone(),
        )))
        .await
        .unwrap();
    let second = instance
        .client
        .submit(ProcessCommand::Start(gated(
            "never",
            vec![claim("a")],
            tx.clone(),
            gate.clone(),
        )))
        .await
        .unwrap();
    instance
        .client
        .submit(ProcessCommand::Start(gated(
            "third",
            vec![claim("a")],
            tx,
            gate.clone(),
        )))
        .await
        .unwrap();
    assert_eq!(next(&mut rx).await, "first");
    instance
        .client
        .call(ProcessCommand::Cancel(first))
        .await
        .unwrap();
    instance
        .client
        .call(ProcessCommand::Cancel(second))
        .await
        .unwrap();
    let snapshot = instance.client.query(()).await.unwrap();
    assert_eq!(snapshot.workflows[0].status, WorkflowStatus::Cancelling);
    assert_eq!(snapshot.workflows[1].status, WorkflowStatus::Cancelled);
    assert!(rx.try_recv().is_err());
    gate.add_permits(1);
    assert_eq!(next(&mut rx).await, "third");
    gate.add_permits(1);
    instance.control.drain().await.unwrap();
}

#[tokio::test]
async fn panic_fences_only_held_objects_and_waiter_does_not_expand_fence() {
    let instance = manager();
    let error = instance
        .client
        .call(ProcessCommand::Start(WorkflowTask::new(
            "panic",
            vec![claim("a")],
            |_| async { panic!("test panic") },
        )))
        .await;
    assert!(matches!(
        error,
        Err(CallError::Business(WorkflowError::NeedsRecovery(_)))
    ));
    let blocked = instance
        .client
        .call(ProcessCommand::Start(WorkflowTask::new(
            "ab",
            vec![claim("a"), claim("b")],
            |_| async { Ok(()) },
        )))
        .await;
    assert!(blocked.is_err());
    instance
        .client
        .call(ProcessCommand::Start(WorkflowTask::new(
            "b",
            vec![claim("b")],
            |_| async { Ok(()) },
        )))
        .await
        .unwrap();
    instance.control.stop().await.unwrap();
}

#[tokio::test]
async fn pause_gates_next_step_and_stop_cancels_timer() {
    let instance = manager();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (a, b) = (started.clone(), release.clone());
    let id = instance
        .client
        .submit(ProcessCommand::Start(WorkflowTask::new(
            "timer",
            vec![],
            move |ctx| async move {
                a.notify_one();
                b.notified().await;
                ctx.checkpoint().await?;
                ctx.sleep(Duration::from_secs(3600)).await
            },
        )))
        .await
        .unwrap();
    started.notified().await;
    instance.control.pause().await.unwrap();
    release.notify_one();
    assert!(
        instance
            .client
            .submit(ProcessCommand::Start(WorkflowTask::new(
                "rejected",
                vec![],
                |_| async { Ok(()) }
            )))
            .await
            .is_err()
    );
    assert!(
        instance.client.query(()).await.unwrap().workflows[0]
            .result
            .is_none()
    );
    instance.control.resume().await.unwrap();
    instance
        .client
        .call(ProcessCommand::Cancel(id))
        .await
        .unwrap();
    idle(&instance).await;
    assert_eq!(
        instance.client.query(()).await.unwrap().workflows[0].status,
        WorkflowStatus::Cancelled
    );
    instance.control.stop().await.unwrap();
}

struct Documents {
    gate: Arc<Semaphore>,
    entered: Arc<Notify>,
    calls: Arc<AtomicUsize>,
    uncertain: bool,
}
#[async_trait]
impl ActivityHandler for Documents {
    type Request = String;
    type Reply = ();
    type Query = ();
    type QueryReply = usize;
    type Error = WorkflowError;
    fn claims(id: &String) -> Vec<ObjectClaim> {
        vec![claim(id)]
    }
    fn name(_: &String) -> &'static str {
        "document.save"
    }
    fn query(&self, _: ()) -> Result<usize, WorkflowError> {
        Ok(self.calls.load(Ordering::SeqCst))
    }
    async fn execute(&self, _: String, _: CancellationToken) -> Result<(), WorkflowError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        self.gate.acquire().await.unwrap().forget();
        if self.uncertain {
            Err(WorkflowError::NeedsRecovery("unknown write result".into()))
        } else {
            Ok(())
        }
    }
}
fn document_runtime(
    scope: ExecutionScope,
    uncertain: bool,
) -> (
    ServiceInstance<ActivityService<Documents>>,
    ActivityClient<Documents>,
    Arc<Semaphore>,
    Arc<Notify>,
) {
    let gate = Arc::new(Semaphore::new(0));
    let entered = Arc::new(Notify::new());
    let service = spawn_service(
        ActivityService::new(Documents {
            gate: gate.clone(),
            entered: entered.clone(),
            calls: Arc::new(AtomicUsize::new(0)),
            uncertain,
        }),
        ServiceConfig::default(),
    );
    let client = ActivityClient::new(service.client.clone(), scope);
    (service, client, gate, entered)
}

#[tokio::test]
async fn dropped_activity_response_still_holds_objects_until_worker_settles() {
    let scope = ExecutionScope::default();
    let instance = spawn_service(ProcessManager::new(scope.clone()), ServiceConfig::default());
    let (worker, client, gate, entered) = document_runtime(scope, false);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let first_client = client.clone();
    let entered_copy = entered.clone();
    instance.client.submit(ProcessCommand::Start(WorkflowTask::new("drop-wait", vec![claim("a")], move |mut ctx| async move {
        {
            let call = first_client.call(&mut ctx, "a".into()); tokio::pin!(call);
            tokio::select! { _ = &mut call => panic!("must block"), _ = entered_copy.notified() => {} }
        }
        tx.send("body-returned").unwrap(); Ok(())
    }))).await.unwrap();
    assert_eq!(next(&mut rx).await, "body-returned");
    let second = client.clone();
    instance
        .client
        .submit(ProcessCommand::Start(WorkflowTask::new(
            "next",
            vec![claim("a")],
            move |mut ctx| async move { second.call(&mut ctx, "a".into()).await },
        )))
        .await
        .unwrap();
    assert_eq!(client.query(()).await.unwrap(), 1);
    assert!(
        instance.client.query(()).await.unwrap().workflows[0]
            .result
            .is_none()
    );
    gate.add_permits(1);
    entered.notified().await;
    assert_eq!(client.query(()).await.unwrap(), 2);
    gate.add_permits(1);
    instance.control.drain().await.unwrap();
    worker.control.stop().await.unwrap();
}

#[tokio::test]
async fn ignored_uncertain_activity_error_still_fences_the_workflow() {
    let scope = ExecutionScope::default();
    let instance = spawn_service(ProcessManager::new(scope.clone()), ServiceConfig::default());
    let (worker, client, gate, _) = document_runtime(scope, true);
    gate.add_permits(1);
    let error = instance
        .client
        .call(ProcessCommand::Start(WorkflowTask::new(
            "ignored-error",
            vec![claim("a")],
            move |mut ctx| async move {
                let _ = client.call(&mut ctx, "a".into()).await;
                Ok(())
            },
        )))
        .await;
    assert!(matches!(
        error,
        Err(CallError::Business(WorkflowError::NeedsRecovery(_)))
    ));
    instance.control.stop().await.unwrap();
    worker.control.stop().await.unwrap();
}

#[tokio::test]
async fn domain_grant_rejects_missing_shared_only_and_foreign_pool_access() {
    let scope = ExecutionScope::default();
    let instance = spawn_service(ProcessManager::new(scope.clone()), ServiceConfig::default());
    let (worker, client, _, _) = document_runtime(scope, false);
    for claims in [
        vec![],
        vec![ObjectClaim::shared(ObjectKey::new("document", "a"))],
        vec![claim("b")],
    ] {
        let client = client.clone();
        let result = instance
            .client
            .call(ProcessCommand::Start(WorkflowTask::new(
                "invalid",
                claims,
                move |mut ctx| async move { client.call(&mut ctx, "a".into()).await },
            )))
            .await;
        assert!(matches!(
            result,
            Err(CallError::Business(WorkflowError::Failed(_)))
        ));
    }
    let foreign = manager();
    let c = client.clone();
    assert!(
        foreign
            .client
            .call(ProcessCommand::Start(WorkflowTask::new(
                "foreign",
                vec![claim("a")],
                move |mut ctx| async move { c.call(&mut ctx, "a".into()).await }
            )))
            .await
            .is_err()
    );
    assert_eq!(client.query(()).await.unwrap(), 0);
    instance.control.stop().await.unwrap();
    foreign.control.stop().await.unwrap();
    worker.control.stop().await.unwrap();
}

#[tokio::test]
async fn manager_and_domain_have_independent_pause_and_observation() {
    let scope = ExecutionScope::default();
    let instance = spawn_service(ProcessManager::new(scope.clone()), ServiceConfig::default());
    let (worker, client, gate, entered) = document_runtime(scope, false);
    let c = client.clone();
    instance
        .client
        .submit(ProcessCommand::Start(WorkflowTask::new(
            "save",
            vec![claim("a")],
            move |mut ctx| async move { c.call(&mut ctx, "a".into()).await },
        )))
        .await
        .unwrap();
    entered.notified().await;
    instance.control.pause().await.unwrap();
    assert_eq!(worker.observer.snapshot().lifecycle, Lifecycle::Running);
    assert_eq!(client.query(()).await.unwrap(), 1);
    gate.add_permits(1);
    idle(&instance).await;
    instance.control.resume().await.unwrap();
    worker.control.pause().await.unwrap();
    let c = client.clone();
    assert!(
        instance
            .client
            .call(ProcessCommand::Start(WorkflowTask::new(
                "paused-worker",
                vec![claim("a")],
                move |mut ctx| async move { c.call(&mut ctx, "a".into()).await }
            )))
            .await
            .is_err()
    );
    assert_eq!(instance.observer.snapshot().lifecycle, Lifecycle::Running);
    instance.control.stop().await.unwrap();
    worker.control.stop().await.unwrap();
}

#[tokio::test]
async fn completed_history_is_bounded_and_immediate_tasks_do_not_stall() {
    let instance = manager();
    for _ in 0..300 {
        instance
            .client
            .call(ProcessCommand::Start(WorkflowTask::new(
                "noop",
                vec![],
                |_| async { Ok(()) },
            )))
            .await
            .unwrap();
    }
    assert_eq!(
        instance.client.query(()).await.unwrap().workflows.len(),
        256
    );
    instance.control.stop().await.unwrap();
}

#[tokio::test]
async fn independent_objects_execute_concurrently_in_the_same_domain_runtime() {
    let scope = ExecutionScope::default();
    let instance = spawn_service(ProcessManager::new(scope.clone()), ServiceConfig::default());
    let (worker, client, gate, entered) = document_runtime(scope, false);
    for key in ["a", "b"] {
        let c = client.clone();
        instance
            .client
            .submit(ProcessCommand::Start(WorkflowTask::new(
                key,
                vec![claim(key)],
                move |mut ctx| async move { c.call(&mut ctx, key.into()).await },
            )))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
    }
    assert_eq!(client.query(()).await.unwrap(), 2);
    assert_eq!(worker.observer.snapshot().service.running, 2);
    gate.add_permits(2);
    instance.control.drain().await.unwrap();
    worker.control.stop().await.unwrap();
}

#[tokio::test]
async fn workflow_panic_waits_for_an_already_dispatched_activity() {
    let scope = ExecutionScope::default();
    let instance = spawn_service(ProcessManager::new(scope.clone()), ServiceConfig::default());
    let (worker, client, gate, entered) = document_runtime(scope, false);
    let entered_copy = entered.clone();
    let c = client.clone();
    instance.client.submit(ProcessCommand::Start(WorkflowTask::new("panic-after-dispatch", vec![claim("a")], move |mut ctx| async move {
        let call = c.call(&mut ctx, "a".into()); tokio::pin!(call);
        tokio::select! { _ = &mut call => panic!("expected pending"), _ = entered_copy.notified() => panic!("workflow panic") }
    }))).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while client.query(()).await.unwrap() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let id = instance
        .client
        .submit(ProcessCommand::Start(WorkflowTask::new(
            "same-object",
            vec![claim("a")],
            |_| async { Ok(()) },
        )))
        .await
        .unwrap();
    assert!(
        instance
            .client
            .query(())
            .await
            .unwrap()
            .workflows
            .iter()
            .all(|w| w.result.is_none())
    );
    gate.add_permits(1);
    idle(&instance).await;
    let snapshot = instance.client.query(()).await.unwrap();
    assert!(
        snapshot
            .workflows
            .iter()
            .all(|w| w.status == WorkflowStatus::NeedsRecovery)
    );
    assert_eq!(snapshot.workflows[1].id, id);
    instance.control.stop().await.unwrap();
    worker.control.stop().await.unwrap();
}

#[tokio::test]
async fn a_dropped_waiter_cannot_dispatch_a_second_activity_under_the_same_grant() {
    let scope = ExecutionScope::default();
    let instance = spawn_service(ProcessManager::new(scope.clone()), ServiceConfig::default());
    let (worker, client, gate, entered) = document_runtime(scope, false);
    let c = client.clone();
    let ready = Arc::new(Notify::new());
    let ready_copy = ready.clone();
    instance.client.submit(ProcessCommand::Start(WorkflowTask::new("drop-then-retry", vec![claim("a")], move |mut ctx| async move {
        {
            let call = c.call(&mut ctx, "a".into()); tokio::pin!(call);
            tokio::select! { _ = &mut call => panic!("expected pending"), _ = entered.notified() => {} }
        }
        assert!(matches!(c.call(&mut ctx, "a".into()).await, Err(WorkflowError::Failed(_))));
        ready_copy.notify_one(); Ok(())
    }))).await.unwrap();
    ready.notified().await;
    assert_eq!(client.query(()).await.unwrap(), 1);
    gate.add_permits(1);
    instance.control.drain().await.unwrap();
    worker.control.stop().await.unwrap();
}

#[tokio::test]
async fn different_namespaces_with_equal_ids_do_not_conflict() {
    let instance = manager();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let gate = Arc::new(Semaphore::new(0));
    for namespace in ["node", "disk"] {
        instance
            .client
            .submit(ProcessCommand::Start(gated(
                namespace,
                vec![ObjectClaim::exclusive(ObjectKey::new(namespace, "same"))],
                tx.clone(),
                gate.clone(),
            )))
            .await
            .unwrap();
    }
    assert_eq!(next(&mut rx).await, "node");
    assert_eq!(next(&mut rx).await, "disk");
    gate.add_permits(2);
    instance.control.drain().await.unwrap();
}

#[tokio::test]
async fn aborted_worker_reports_uncertainty_and_does_not_unlock_for_another_writer() {
    let scope = ExecutionScope::default();
    let instance = spawn_service(ProcessManager::new(scope.clone()), ServiceConfig::default());
    let (mut worker, client, _, entered) = document_runtime(scope, false);
    instance
        .client
        .submit(ProcessCommand::Start(WorkflowTask::new(
            "save",
            vec![claim("a")],
            move |mut ctx| async move { client.call(&mut ctx, "a".into()).await },
        )))
        .await
        .unwrap();
    entered.notified().await;
    worker.task.abort();
    idle(&instance).await;
    assert_eq!(
        instance.client.query(()).await.unwrap().workflows[0].status,
        WorkflowStatus::NeedsRecovery
    );
    assert!(
        instance
            .client
            .call(ProcessCommand::Start(WorkflowTask::new(
                "blocked",
                vec![claim("a")],
                |_| async { Ok(()) }
            )))
            .await
            .is_err()
    );
    instance.control.stop().await.unwrap();
}
