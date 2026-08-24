use std::{collections::HashMap, time::Duration};

use mdc_runtime::{
    ServiceShutdown, TaskState, TaskVisibility,
    demo::{
        BgBackend, BgId, CancelRebuildRequest, DemoBlueprint, DemoCatalog, DemoError, DiskId,
        DiskOfflineRequest, RebuildStatus, ResumeRebuildRequest, ServiceKind,
        SuspendRebuildRequest, demo_context,
    },
    request_channel,
};

fn catalog(entries: &[(&str, &[&str])]) -> DemoCatalog {
    DemoCatalog::new(
        entries.iter().map(|(disk, bgs)| {
            (
                DiskId::new(*disk),
                bgs.iter().map(|bg| BgId::new(*bg)).collect(),
            )
        }),
        Duration::from_millis(2),
    )
}

#[tokio::test]
async fn two_disks_share_one_bg_and_one_public_campaign() {
    let backend = BgBackend::new(Duration::from_millis(5));
    let system = DemoBlueprint::install()
        .unwrap()
        .spawn(
            catalog(&[
                ("disk-1", &["bg-a", "bg-shared"]),
                ("disk-2", &["bg-shared", "bg-b"]),
            ]),
            backend.clone(),
            2,
        )
        .await
        .unwrap();
    let mut task_events = system.rebuild.observer.watch_tasks();

    let (disk_1, disk_2) = tokio::join!(
        system.disk_offline(DiskId::new("disk-1"), 1),
        system.disk_offline(DiskId::new("disk-2"), 2),
    );
    assert_eq!(disk_1.unwrap(), Ok(RebuildStatus::Completed));
    assert_eq!(disk_2.unwrap(), Ok(RebuildStatus::Completed));

    let history = backend.snapshot();
    let counts = history
        .started
        .into_iter()
        .fold(HashMap::new(), |mut map, bg| {
            *map.entry(bg).or_insert(0usize) += 1;
            map
        });
    assert_eq!(counts.len(), 3);
    assert_eq!(counts.get(&BgId::new("bg-shared")), Some(&1));

    let mut public_campaigns = 0;
    while let Ok(event) = task_events.try_recv() {
        if event.visibility == TaskVisibility::Public && event.state == TaskState::Started {
            public_campaigns += 1;
        }
    }
    assert_eq!(public_campaigns, 1);
    assert_eq!(
        system.rebuild_snapshot(10).await.unwrap().campaigns_started,
        1
    );
    system
        .services
        .shutdown_all(ServiceShutdown::Immediate)
        .await
        .unwrap();
}

#[tokio::test]
async fn suspend_waits_for_inflight_and_resume_refills_window() {
    let backend = BgBackend::new(Duration::from_millis(20));
    let system = DemoBlueprint::install()
        .unwrap()
        .spawn(
            catalog(&[("disk-1", &["bg-1", "bg-2", "bg-3", "bg-4", "bg-5"])]),
            backend.clone(),
            2,
        )
        .await
        .unwrap();

    let (disk_completed, disk_ticket) = request_channel();
    system
        .services
        .router()
        .send_payload(
            ServiceKind::Event,
            DiskOfflineRequest {
                disk: DiskId::new("disk-1"),
                completed: disk_completed,
            },
            demo_context(20),
        )
        .await
        .unwrap();

    wait_until(|| backend.snapshot().started.len() == 2).await;
    let (suspend_completed, suspend_ticket) = request_channel();
    system
        .services
        .router()
        .send_payload(
            ServiceKind::Rebuild,
            SuspendRebuildRequest {
                completed: suspend_completed,
            },
            demo_context(21),
        )
        .await
        .unwrap();
    assert_eq!(suspend_ticket.await.unwrap(), Ok(()));
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(backend.snapshot().started.len(), 2);
    let suspended = system.rebuild_snapshot(22).await.unwrap();
    assert!(suspended.suspended);
    assert_eq!(suspended.running_bgs, 0);
    assert_eq!(suspended.queued_bgs, 3);

    let (resume_completed, resume_ticket) = request_channel();
    system
        .services
        .router()
        .send_payload(
            ServiceKind::Rebuild,
            ResumeRebuildRequest {
                completed: resume_completed,
            },
            demo_context(23),
        )
        .await
        .unwrap();
    assert_eq!(resume_ticket.await.unwrap(), Ok(()));
    assert_eq!(disk_ticket.await.unwrap(), Ok(RebuildStatus::Completed));
    assert_eq!(backend.snapshot().started.len(), 5);
    system
        .services
        .shutdown_all(ServiceShutdown::Immediate)
        .await
        .unwrap();
}

#[tokio::test]
async fn cancelling_one_disk_keeps_a_shared_bg_needed_by_another_disk() {
    let backend = BgBackend::new(Duration::from_millis(20));
    let system = DemoBlueprint::install()
        .unwrap()
        .spawn(
            catalog(&[
                ("disk-1", &["bg-a", "bg-shared"]),
                ("disk-2", &["bg-shared", "bg-b"]),
            ]),
            backend.clone(),
            1,
        )
        .await
        .unwrap();

    let (disk_1_reply, disk_1_ticket) = request_channel();
    let (disk_2_reply, disk_2_ticket) = request_channel();
    let router = system.services.router();
    router
        .send_payload(
            ServiceKind::Event,
            DiskOfflineRequest {
                disk: DiskId::new("disk-1"),
                completed: disk_1_reply,
            },
            demo_context(30),
        )
        .await
        .unwrap();
    router
        .send_payload(
            ServiceKind::Event,
            DiskOfflineRequest {
                disk: DiskId::new("disk-2"),
                completed: disk_2_reply,
            },
            demo_context(31),
        )
        .await
        .unwrap();
    wait_until_async(|| async { system.rebuild_snapshot(32).await.unwrap().active_disks == 2 })
        .await;

    let (cancel_reply, cancel_ticket) = request_channel();
    router
        .send_payload(
            ServiceKind::Rebuild,
            CancelRebuildRequest {
                disk: DiskId::new("disk-1"),
                completed: cancel_reply,
            },
            demo_context(33),
        )
        .await
        .unwrap();
    assert_eq!(cancel_ticket.await.unwrap(), Ok(()));
    assert_eq!(disk_1_ticket.await.unwrap(), Err(DemoError::Cancelled));
    assert_eq!(disk_2_ticket.await.unwrap(), Ok(RebuildStatus::Completed));

    let history = backend.snapshot();
    assert_eq!(
        history
            .started
            .iter()
            .filter(|bg| **bg == BgId::new("bg-shared"))
            .count(),
        1
    );
    assert!(!history.cancelled.contains(&BgId::new("bg-shared")));
    system
        .services
        .shutdown_all(ServiceShutdown::Immediate)
        .await
        .unwrap();
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

async fn wait_until_async<F, Fut>(mut predicate: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    tokio::time::timeout(Duration::from_secs(2), async move {
        while !predicate().await {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("condition timed out");
}
