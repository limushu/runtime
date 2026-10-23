#[allow(dead_code)]
mod support;
use async_trait::async_trait;
use std::sync::Arc;
use support::*;
use tokio::sync::{Semaphore, mpsc};
use workflow_foundation::*;

struct Steps {
    service: ServiceClient<Probe>,
    work: Vec<Work>,
}
#[async_trait]
impl Workflow for Steps {
    type Output = ();
    const NAME: &'static str = "steps";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        for work in self.work {
            ctx.activity(&self.service, work).await?;
        }
        Ok(())
    }
}

async fn observed(
    observer: &mut ExecutionObserver,
    predicate: impl Fn(&ExecutionSnapshot) -> bool,
) -> ExecutionSnapshot {
    tokio::time::timeout(LIMIT, async {
        loop {
            let snapshot = observer.snapshot();
            if predicate(&snapshot) {
                return snapshot;
            }
            observer.changed().await.unwrap();
        }
    })
    .await
    .expect("observation timeout")
}

#[tokio::test]
async fn live_and_historical_snapshots_retain_each_actual_step() {
    let (mut pool, service, mut entered) = setup();
    let gate = Arc::new(Semaphore::new(0));
    let mut run = pool
        .submit(Steps {
            service,
            work: vec![
                Work::write("prepare", "a"),
                Work::write("publish", "b").gated(&gate),
            ],
        })
        .await
        .unwrap();
    assert_eq!(next(&mut entered).await, "prepare");
    assert_eq!(next(&mut entered).await, "publish");
    let client = pool.client();
    let snapshot = client.execution(run.id()).unwrap();
    assert_eq!(snapshot.activities.total, 2);
    assert_eq!(snapshot.activities.omitted, 0);
    let first = &snapshot.activities.entries[0];
    assert_eq!(first.name, "prepare");
    assert_eq!(first.service, "probe");
    assert_eq!(first.resources, vec![write("a")]);
    assert_eq!(first.state, ActivityState::Completed);
    assert!(first.started_at.is_some());
    assert!(first.finished_at.is_some());
    let second = &snapshot.activities.entries[1];
    assert_eq!(second.id.workflow, run.id());
    assert_eq!(second.id.sequence, 2);
    assert_eq!(second.state, ActivityState::Running);
    assert!(second.finished_at.is_none());
    let mut observer = client.observe_execution(run.id()).unwrap();
    gate.add_permits(1);
    finish(&mut run).await.unwrap();
    let terminal = observed(&mut observer, |s| s.state == RunState::Completed).await;
    assert!(terminal.activities.entries.iter().all(|a| {
        a.state == ActivityState::Completed && a.started_at.is_some() && a.finished_at.is_some()
    }));
    let id = run.id();
    drop(run);
    pool.shutdown().await.unwrap();
    assert_eq!(
        client.execution(id).unwrap().activities,
        terminal.activities
    );
}

#[tokio::test]
async fn observer_wakes_when_service_starts_without_workflow_state_change() {
    // Deliberately do not poll the service until the workflow has dispatched.
    let mut driver = WorkflowDriver::new("p", Config::default()).unwrap();
    let (tx, mut entered) = mpsc::unbounded_channel();
    let service = driver.register("probe", Probe::new(tx)).unwrap();
    let gate = Arc::new(Semaphore::new(0));
    let mut run = driver
        .submit(Call::new(&service, Work::write("work", "r").gated(&gate)))
        .unwrap();
    let mut observer = driver.view().observe_execution(run.id()).unwrap();
    // tick has no completed workflow, but polls its future through dispatch.
    // A single-threaded test prevents the spawned service from running during this poll.
    for _ in 0..32 {
        if observer
            .snapshot()
            .activities
            .entries
            .back()
            .is_some_and(|a| a.state == ActivityState::Queued)
        {
            break;
        }
        let tick = driver.tick();
        tokio::pin!(tick);
        let _ = futures_util::poll!(&mut tick);
    }
    assert_eq!(
        observer.snapshot().activities.entries[0].state,
        ActivityState::Queued
    );
    let state = observer.snapshot().state;
    assert!(matches!(state, RunState::Activity(_)));
    // Consume existing notifications before the service gets CPU time.
    for _ in 0..32 {
        let changed = observer.changed();
        tokio::pin!(changed);
        if futures_util::poll!(&mut changed).is_pending() {
            break;
        }
    }
    {
        let changed = observer.changed();
        tokio::pin!(changed);
        assert!(futures_util::poll!(&mut changed).is_pending());
        next(&mut entered).await;
        let snapshot = tokio::time::timeout(LIMIT, changed).await.unwrap().unwrap();
        assert_eq!(snapshot.activities.entries[0].state, ActivityState::Running);
        assert_eq!(snapshot.state, state);
    }
    gate.add_permits(1);
    while !driver.is_idle() {
        driver.tick().await;
    }
    finish(&mut run).await.unwrap();
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn resource_wait_cancellation_is_recorded_without_starting_the_activity() {
    let (mut pool, service, mut entered) = setup();
    let gate = Arc::new(Semaphore::new(0));
    let mut first = pool
        .submit(Call::new(&service, Work::write("owner", "r").gated(&gate)))
        .await
        .unwrap();
    next(&mut entered).await;
    let mut waiting = pool
        .submit(Call::new(&service, Work::write("waiting", "r")))
        .await
        .unwrap();
    let mut observer = pool.client().observe_execution(waiting.id()).unwrap();
    let snapshot = observed(&mut observer, |s| s.state == RunState::WaitingResources).await;
    assert_eq!(
        snapshot.activities.entries[0].state,
        ActivityState::WaitingResources
    );
    waiting.control.cancel();
    assert_eq!(finish(&mut waiting).await, Err(Failure::Cancelled));
    let record = pool
        .client()
        .execution(waiting.id())
        .unwrap()
        .activities
        .entries[0]
        .clone();
    assert_eq!(record.state, ActivityState::Failed(Failure::Cancelled));
    assert!(record.started_at.is_none());
    assert!(record.finished_at.is_some());
    gate.add_permits(1);
    finish(&mut first).await.unwrap();
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancellation_request_keeps_running_until_real_settlement() {
    for behavior in [Behavior::Success, Behavior::CancelThenSettle] {
        let (mut pool, service, mut entered) = setup();
        let gate = Arc::new(Semaphore::new(0));
        let mut run = pool
            .submit(Call::new(
                &service,
                Work::write("commit", "r").gated(&gate).behavior(behavior),
            ))
            .await
            .unwrap();
        next(&mut entered).await;
        run.control.cancel();
        if matches!(behavior, Behavior::CancelThenSettle) {
            assert_eq!(next(&mut entered).await, "cancel-observed");
        }
        let snapshot = pool.client().execution(run.id()).unwrap();
        assert!(snapshot.cancellation_requested);
        assert_eq!(snapshot.activities.entries[0].state, ActivityState::Running);
        assert!(snapshot.activities.entries[0].finished_at.is_none());
        gate.add_permits(1);
        let result = finish(&mut run).await;
        let expected = if matches!(behavior, Behavior::Success) {
            assert!(result.is_ok());
            ActivityState::Completed
        } else {
            assert_eq!(result, Err(Failure::Cancelled));
            ActivityState::Failed(Failure::Cancelled)
        };
        assert_eq!(
            pool.client()
                .execution(run.id())
                .unwrap()
                .activities
                .entries[0]
                .state,
            expected
        );
        pool.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn failure_panic_and_unavailability_preserve_activity_outcomes() {
    for behavior in [Behavior::Failed, Behavior::Uncertain, Behavior::Panic] {
        let (mut pool, service, _) = setup();
        let mut run = pool
            .submit(Call::new(
                &service,
                Work::write("failure", "r").behavior(behavior),
            ))
            .await
            .unwrap();
        let error = finish(&mut run).await.unwrap_err();
        let snapshot = pool.client().execution(run.id()).unwrap();
        let activity = &snapshot.activities.entries[0];
        match (&error, &activity.state) {
            (Failure::Failed(_), ActivityState::Failed(Failure::Failed(_)))
            | (Failure::Uncertain(_), ActivityState::Failed(Failure::Uncertain(_))) => {}
            _ => panic!("unexpected outcome: {snapshot:?}"),
        }
        assert!(activity.started_at.is_some());
        assert!(activity.finished_at.is_some());
        pool.shutdown().await.unwrap();
    }
    let (mut pool, service, _) = setup();
    service.shutdown().await.unwrap();
    let mut run = pool
        .submit(Call::new(&service, Work::write("unavailable", "r")))
        .await
        .unwrap();
    assert_eq!(finish(&mut run).await, Err(Failure::Unavailable));
    let activity = pool
        .client()
        .execution(run.id())
        .unwrap()
        .activities
        .entries[0]
        .clone();
    assert_eq!(activity.state, ActivityState::Failed(Failure::Unavailable));
    assert!(activity.started_at.is_none());
    assert!(activity.finished_at.is_some());
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn bounded_history_reports_omissions_and_survives_task_eviction_via_observer() {
    let mut pool = WorkflowRuntime::start(
        "p",
        Config {
            task_history: 1,
            activity_history: 2,
            ..Config::default()
        },
    )
    .unwrap();
    let (tx, _) = mpsc::unbounded_channel();
    let service = pool.register("probe", Probe::new(tx)).unwrap();
    let mut first = pool
        .submit(Steps {
            service: service.clone(),
            work: (0..5).map(|_| Work::write("step", "r")).collect(),
        })
        .await
        .unwrap();
    let observer = pool.client().observe_execution(first.id()).unwrap();
    finish(&mut first).await.unwrap();
    let history = observer.snapshot().activities;
    assert_eq!(history.total, 5);
    assert_eq!(history.omitted, 3);
    assert_eq!(
        history
            .entries
            .iter()
            .map(|a| a.id.sequence)
            .collect::<Vec<_>>(),
        vec![4, 5]
    );
    let mut last = pool
        .submit(Call::new(&service, Work::write("last", "r")))
        .await
        .unwrap();
    finish(&mut last).await.unwrap();
    tokio::time::timeout(LIMIT, async {
        while pool.client().execution(first.id()).is_ok() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(observer.snapshot().activities, history);
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn zero_activity_history_is_rejected() {
    assert!(
        WorkflowRuntime::start(
            "p",
            Config {
                activity_history: 0,
                ..Config::default()
            }
        )
        .is_err()
    );
}

#[tokio::test]
async fn forcefully_dropped_host_marks_started_activity_uncertain() {
    let (pool, service, mut entered) = setup();
    let gate = Arc::new(Semaphore::new(0));
    let mut run = pool
        .submit(Call::new(
            &service,
            Work::write("started", "r").gated(&gate),
        ))
        .await
        .unwrap();
    next(&mut entered).await;
    let observer = pool.client().observe_execution(run.id()).unwrap();
    drop(pool);
    assert!(matches!(finish(&mut run).await, Err(Failure::Uncertain(_))));
    // Forced service abort is asynchronous; the activity becomes terminal when its guard drops.
    tokio::time::timeout(LIMIT, async {
        while observer.snapshot().activities.entries[0]
            .finished_at
            .is_none()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let activity = observer.snapshot().activities.entries[0].clone();
    assert!(matches!(
        activity.state,
        ActivityState::Failed(Failure::Uncertain(_))
    ));
    assert!(activity.started_at.is_some());
}

#[tokio::test]
async fn queued_shutdown_records_cancellation_without_starting_work() {
    let mut driver = WorkflowDriver::new("p", Config::default()).unwrap();
    let (tx, mut entered) = mpsc::unbounded_channel();
    let service = driver.register("probe", Probe::new(tx)).unwrap();
    let mut run = driver
        .submit(Call::new(&service, Work::write("queued", "r")))
        .unwrap();
    let observer = driver.view().observe_execution(run.id()).unwrap();
    for _ in 0..32 {
        if observer
            .snapshot()
            .activities
            .entries
            .back()
            .is_some_and(|a| a.state == ActivityState::Queued)
        {
            break;
        }
        let tick = driver.tick();
        tokio::pin!(tick);
        let _ = futures_util::poll!(&mut tick);
    }
    assert_eq!(
        observer.snapshot().activities.entries[0].state,
        ActivityState::Queued
    );
    service.shutdown().await.unwrap();
    while !driver.is_idle() {
        driver.tick().await;
    }
    assert_eq!(finish(&mut run).await, Err(Failure::Cancelled));
    let activity = observer.snapshot().activities.entries[0].clone();
    assert_eq!(activity.state, ActivityState::Failed(Failure::Cancelled));
    assert!(activity.started_at.is_none());
    assert!(activity.finished_at.is_some());
    assert!(entered.try_recv().is_err());
    driver.shutdown().await.unwrap();
}
