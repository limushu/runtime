use std::time::{Duration, Instant};

use mdc_runtime::{
    CancelReason, Submission, TaskExit, TaskState,
    demo::{BgBackend, DemoPool, DiskId, DiskRequest, DiskResponse, DiskState},
};

#[tokio::test]
async fn cancellation_waits_for_the_complete_downstream_chain() {
    let backend = BgBackend::new(Duration::from_secs(5), Duration::from_millis(60));
    let pool = DemoPool::start_with_backend([DiskId::new("disk-1")], backend.clone())
        .await
        .unwrap();

    let Submission::Task(ticket) = pool
        .disk
        .client
        .submit(DiskRequest::Offline(DiskId::new("disk-1")))
        .await
        .unwrap()
    else {
        panic!("offline must create a task");
    };
    wait_until(|| !backend.snapshot().started.is_empty()).await;

    let disk_task = pool.disk.observer.task_snapshots().remove(0);
    let rebuild_task = pool.rebuild.observer.task_snapshots().remove(0);
    let bg_task = pool.bg.observer.task_snapshots().remove(0);
    assert_eq!(disk_task.operation_id, rebuild_task.operation_id);
    assert_eq!(rebuild_task.operation_id, bg_task.operation_id);
    assert_eq!(rebuild_task.trace.parent_task, Some(disk_task.task_id));
    assert_eq!(bg_task.trace.parent_task, Some(rebuild_task.task_id));

    let started = Instant::now();
    let exit = ticket
        .cancel_and_wait(CancelReason::requested("operator cancel"))
        .await
        .unwrap();
    assert!(started.elapsed() >= Duration::from_millis(50));
    assert!(matches!(exit, TaskExit::Cancelled(_)));

    let backend = backend.snapshot();
    assert_eq!(backend.cancel_requested, vec![DiskId::new("disk-1")]);
    assert_eq!(backend.cancelled, vec![DiskId::new("disk-1")]);
    assert!(pool.disk.observer.task_snapshots().is_empty());
    assert!(pool.rebuild.observer.task_snapshots().is_empty());
    assert!(pool.bg.observer.task_snapshots().is_empty());
    assert_eq!(
        pool.query_disk(DiskId::new("disk-1"))
            .await
            .unwrap()
            .unwrap()
            .state,
        DiskState::Online
    );
}

#[tokio::test]
async fn replacement_stays_queued_until_the_old_task_is_gracefully_cancelled() {
    let backend = BgBackend::new(Duration::from_secs(5), Duration::from_millis(80));
    let pool = DemoPool::start_with_backend([DiskId::new("disk-1")], backend.clone())
        .await
        .unwrap();

    let Submission::Task(offline) = pool
        .disk
        .client
        .submit(DiskRequest::Offline(DiskId::new("disk-1")))
        .await
        .unwrap()
    else {
        panic!("offline must create a task");
    };
    wait_until(|| !backend.snapshot().started.is_empty()).await;

    let Submission::Task(fault) = pool
        .disk
        .client
        .submit(DiskRequest::Fault(DiskId::new("disk-1")))
        .await
        .unwrap()
    else {
        panic!("fault must create a task");
    };

    wait_until(|| {
        let tasks = pool.disk.observer.task_snapshots();
        tasks.iter().any(|task| task.state == TaskState::Cancelling)
            && tasks.iter().any(|task| task.state == TaskState::Queued)
    })
    .await;
    assert_eq!(
        pool.query_disk(DiskId::new("disk-1"))
            .await
            .unwrap()
            .unwrap()
            .state,
        DiskState::Offlining
    );

    assert!(matches!(
        offline.wait().await.unwrap(),
        TaskExit::Cancelled(_)
    ));
    assert!(matches!(
        fault.wait().await.unwrap(),
        TaskExit::Completed(DiskResponse::Faulted)
    ));
    assert_eq!(
        pool.query_disk(DiskId::new("disk-1"))
            .await
            .unwrap()
            .unwrap()
            .state,
        DiskState::Faulted
    );
}

#[tokio::test]
async fn query_is_an_immediate_reply_and_never_creates_a_task() {
    let pool = DemoPool::start([DiskId::new("disk-1")]).await.unwrap();
    let mut events = pool.disk.observer.task_events();

    let response = pool
        .disk
        .client
        .submit(DiskRequest::Query(DiskId::new("disk-1")))
        .await
        .unwrap();
    assert!(matches!(
        response,
        Submission::Reply(Ok(DiskResponse::Snapshot(_)))
    ));
    assert!(events.try_recv().is_err());
    assert!(pool.disk.observer.task_snapshots().is_empty());
}

async fn wait_until(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(2), async move {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("condition timed out");
}
