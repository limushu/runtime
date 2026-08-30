use control_runtime::{Activity, ObservationEvent, RuntimeConfig, RuntimeError, ServiceLifecycle};
use pool_control_plane::{
    MemberDiskId, MemberDiskRequest, MemberDiskState, PoolRuntime, VirtualDiskReply,
};
use std::collections::HashSet;
use std::time::Duration;

#[tokio::test]
async fn query_is_a_future_but_not_a_managed_task() {
    let disk = MemberDiskId::new("disk-1");
    let pool = PoolRuntime::new([disk.clone()]);
    assert_eq!(
        pool.member_disk(disk).await.unwrap().state,
        MemberDiskState::Ua
    );
    let snapshot = pool.member_disk_observer().snapshot();
    assert_eq!(snapshot.activity, Activity::Idle);
    assert_eq!(snapshot.active_tasks, 0);
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn duplicate_object_intents_join_one_workflow() {
    let disk = MemberDiskId::new("disk-1");
    let pool = PoolRuntime::new([disk.clone()]);
    let router = pool.router();
    let first = router.call_root("first", MemberDiskRequest::Offline(disk.clone()));
    let second = router.call_root("second", MemberDiskRequest::Offline(disk));
    let (first, second) = tokio::join!(first, second);
    assert_eq!(first.unwrap(), second.unwrap());
    let VirtualDiskReply::Stats(stats) = pool.virtual_disk_stats().await.unwrap() else {
        panic!("expected stats")
    };
    assert_eq!(stats.started, 1);
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn one_batch_operation_drives_independent_disk_intents() {
    let disks = vec![
        MemberDiskId::new("disk-1"),
        MemberDiskId::new("disk-2"),
        MemberDiskId::new("disk-3"),
    ];
    let pool = PoolRuntime::new(disks.clone());
    let mut events = pool.member_disk_observer().subscribe_events();

    let reply = pool.offline_many(disks).await.unwrap();
    assert_eq!(reply.items.len(), 3);
    assert!(reply
        .items
        .iter()
        .all(|item| item.result == Ok(MemberDiskState::Removed)));

    let starts: Vec<_> = std::iter::from_fn(|| events.try_recv().ok())
        .filter_map(|event| match event {
            ObservationEvent::TaskStarted {
                task_attempt_id,
                parent_task_attempt_id,
                operation_id,
                ..
            } => Some((task_attempt_id, parent_task_attempt_id, operation_id)),
            _ => None,
        })
        .collect();
    assert_eq!(starts.len(), 3, "only the three object intents are tasks");
    let operations: HashSet<_> = starts.iter().map(|(_, _, operation)| *operation).collect();
    assert_eq!(operations.len(), 1);
    assert!(starts.iter().all(|(_, parent, _)| parent.is_none()));
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn online_cooperatively_preempts_only_its_disk_intent() {
    let disk = MemberDiskId::new("disk-1");
    let pool = PoolRuntime::new([disk.clone()]);
    let router = pool.router();
    let offline_disk = disk.clone();
    let offline = tokio::spawn(async move {
        router
            .call_root("offline", MemberDiskRequest::Offline(offline_disk))
            .await
    });
    tokio::time::sleep(Duration::from_millis(35)).await;

    let online = pool.online(disk.clone()).await.unwrap();
    assert_eq!(offline.await.unwrap(), Err(RuntimeError::Cancelled));
    assert_eq!(online.state, MemberDiskState::Ua);
    let VirtualDiskReply::Stats(stats) = pool.virtual_disk_stats().await.unwrap() else {
        panic!("expected stats")
    };
    assert_eq!(stats.stable_stops, 1);
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn one_disk_recovery_does_not_cancel_the_rest_of_a_batch() {
    let disk_1 = MemberDiskId::new("disk-1");
    let disk_2 = MemberDiskId::new("disk-2");
    let disk_3 = MemberDiskId::new("disk-3");
    let pool = PoolRuntime::new([disk_1.clone(), disk_2.clone(), disk_3.clone()]);
    let router = pool.router();
    let batch_disks = vec![disk_1.clone(), disk_2.clone(), disk_3.clone()];

    let batch = tokio::spawn(async move {
        router
            .call_batch(
                "offline batch",
                batch_disks.into_iter().map(MemberDiskRequest::Offline),
            )
            .await
    });
    tokio::time::sleep(Duration::from_millis(35)).await;
    assert_eq!(
        pool.online(disk_1.clone()).await.unwrap().state,
        MemberDiskState::Ua
    );

    let response = batch.await.unwrap().unwrap();
    assert_eq!(response[0], Err(RuntimeError::Cancelled));
    assert_eq!(
        response[1].as_ref().unwrap().state,
        MemberDiskState::Removed
    );
    assert_eq!(
        response[2].as_ref().unwrap().state,
        MemberDiskState::Removed
    );
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_new_event_can_replace_the_pending_replacement() {
    let disk = MemberDiskId::new("disk-1");
    let pool = PoolRuntime::new([disk.clone()]);
    let router = pool.router();

    let offline_router = router.clone();
    let offline_disk = disk.clone();
    let offline = tokio::spawn(async move {
        offline_router
            .call_root("offline", MemberDiskRequest::Offline(offline_disk))
            .await
    });
    tokio::time::sleep(Duration::from_millis(35)).await;

    let online_router = router.clone();
    let online_disk = disk.clone();
    let online = tokio::spawn(async move {
        online_router
            .call_root("online", MemberDiskRequest::Online(online_disk))
            .await
    });
    tokio::time::sleep(Duration::from_millis(1)).await;

    let latest = router
        .call_root("offline again", MemberDiskRequest::Offline(disk))
        .await
        .unwrap();
    assert_eq!(offline.await.unwrap(), Err(RuntimeError::Cancelled));
    assert!(matches!(
        online.await.unwrap(),
        Err(RuntimeError::Superseded(_))
    ));
    assert_eq!(latest.state, MemberDiskState::Removed);
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn duplicate_pending_intents_join_before_capacity_is_available() {
    let disk_1 = MemberDiskId::new("disk-1");
    let disk_2 = MemberDiskId::new("disk-2");
    let pool = PoolRuntime::with_config(
        [disk_1.clone(), disk_2.clone()],
        RuntimeConfig {
            max_active_workflows: 1,
            ..RuntimeConfig::default()
        },
    );
    let router = pool.router();

    let first_router = router.clone();
    let first = tokio::spawn(async move {
        first_router
            .call_root("first disk", MemberDiskRequest::Offline(disk_1))
            .await
    });
    tokio::time::sleep(Duration::from_millis(10)).await;

    let pending = router.call_root("pending", MemberDiskRequest::Offline(disk_2.clone()));
    let duplicate = router.call_root("duplicate pending", MemberDiskRequest::Offline(disk_2));
    let (pending, duplicate) = tokio::join!(pending, duplicate);
    assert_eq!(pending.unwrap(), duplicate.unwrap());
    assert!(first.await.unwrap().is_ok());

    let VirtualDiskReply::Stats(stats) = pool.virtual_disk_stats().await.unwrap() else {
        panic!("expected stats")
    };
    assert_eq!(
        stats.started, 2,
        "the pending duplicate must not start twice"
    );
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn pause_rejects_new_business_and_resume_accepts_it() {
    let disk = MemberDiskId::new("disk-1");
    let pool = PoolRuntime::new([disk.clone()]);
    let control = pool.member_disk_control();
    control.pause().await.unwrap();
    assert!(matches!(
        pool.member_disk(disk.clone()).await,
        Err(RuntimeError::ServicePaused(_))
    ));
    control.resume().await.unwrap();
    assert_eq!(
        pool.member_disk(disk).await.unwrap().state,
        MemberDiskState::Ua
    );
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn drain_waits_for_accepted_work_and_finishes_paused() {
    let disk = MemberDiskId::new("disk-1");
    let pool = PoolRuntime::new([disk.clone()]);
    let router = pool.router();
    let control = pool.member_disk_control();
    let observer = pool.member_disk_observer();
    let work = tokio::spawn(async move {
        router
            .call_root("offline", MemberDiskRequest::Offline(disk))
            .await
    });
    tokio::time::sleep(Duration::from_millis(15)).await;
    control.drain().await.unwrap();
    assert!(work.await.unwrap().is_ok());
    assert_eq!(observer.snapshot().lifecycle, ServiceLifecycle::Paused);
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn dropping_a_root_call_cancels_submitted_downstream_work() {
    let disk = MemberDiskId::new("disk-1");
    let pool = PoolRuntime::new([disk.clone()]);
    let router = pool.router();
    let caller = tokio::spawn(async move {
        router
            .call_root("abandoned offline", MemberDiskRequest::Offline(disk))
            .await
    });

    tokio::time::sleep(Duration::from_millis(35)).await;
    caller.abort();
    let _ = caller.await;
    tokio::time::sleep(Duration::from_millis(60)).await;

    let VirtualDiskReply::Stats(stats) = pool.virtual_disk_stats().await.unwrap() else {
        panic!("expected stats")
    };
    assert_eq!(stats.stable_stops, 1);
    assert_eq!(pool.member_disk_observer().snapshot().active_tasks, 0);
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn dropping_a_batch_cancels_every_submitted_item() {
    let disks = vec![MemberDiskId::new("disk-1"), MemberDiskId::new("disk-2")];
    let pool = PoolRuntime::new(disks.clone());
    let router = pool.router();
    let caller = tokio::spawn(async move {
        router
            .call_batch(
                "abandoned offline batch",
                disks.into_iter().map(MemberDiskRequest::Offline),
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(35)).await;
    caller.abort();
    let _ = caller.await;
    tokio::time::sleep(Duration::from_millis(60)).await;

    let VirtualDiskReply::Stats(stats) = pool.virtual_disk_stats().await.unwrap() else {
        panic!("expected stats")
    };
    assert_eq!(stats.stable_stops, 2);
    assert_eq!(pool.member_disk_observer().snapshot().active_tasks, 0);
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn force_aborting_a_service_first_cancels_its_downstream_calls() {
    let disk = MemberDiskId::new("disk-1");
    let pool = PoolRuntime::new([disk.clone()]);
    let router = pool.router();
    let caller = tokio::spawn(async move {
        router
            .call_root("offline before unload", MemberDiskRequest::Offline(disk))
            .await
    });

    tokio::time::sleep(Duration::from_millis(35)).await;
    pool.force_abort_member_disk();
    assert!(matches!(
        caller.await.unwrap(),
        Err(RuntimeError::ChannelClosed(_))
    ));
    tokio::time::sleep(Duration::from_millis(60)).await;

    let VirtualDiskReply::Stats(stats) = pool.virtual_disk_stats().await.unwrap() else {
        panic!("expected stats")
    };
    assert_eq!(stats.stable_stops, 1);
    drop(pool);
}
