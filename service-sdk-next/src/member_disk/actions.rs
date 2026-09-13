use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

use super::{
    DiskOpenResult, DiskStateChange, DiskUuid, IoState, MemberDiskChange, MemberDiskCommit,
    MemberDiskError, MemberDiskMetadata, PoolNodes, PortError, VirtualDisks, now_millis,
    requests::{CommittedChange, RequestCompletion, RequestId, StepResult},
};

#[derive(Clone)]
pub(crate) struct ActionPorts {
    metadata: Arc<dyn MemberDiskMetadata>,
    pool_nodes: Arc<dyn PoolNodes>,
    virtual_disks: Arc<dyn VirtualDisks>,
    retry_delay: Duration,
}

impl ActionPorts {
    pub fn new(
        metadata: Arc<dyn MemberDiskMetadata>,
        pool_nodes: Arc<dyn PoolNodes>,
        virtual_disks: Arc<dyn VirtualDisks>,
        retry_delay: Duration,
    ) -> Self {
        Self {
            metadata,
            pool_nodes,
            virtual_disks,
            retry_delay,
        }
    }
}

#[derive(Clone)]
pub(crate) struct DiskActionInput {
    pub request: RequestId,
    pub disk: DiskUuid,
    pub cancel: CancellationToken,
}

pub(crate) struct WaitInput {
    pub request: RequestId,
    pub observed_at: u64,
    pub recovery_window: Duration,
    pub cancel: CancellationToken,
}

pub(crate) struct AllocationPlan {
    pub request: RequestId,
    pub commit: MemberDiskCommit,
}

pub(crate) struct ReleasePlan {
    pub request: RequestId,
    pub commit: MemberDiskCommit,
}

pub(crate) async fn close_io(
    ports: ActionPorts,
    inputs: Vec<DiskActionInput>,
) -> Vec<RequestCompletion> {
    let states: Vec<_> = inputs
        .iter()
        .map(|input| DiskStateChange {
            disk: input.disk.clone(),
            state: IoState::Down,
        })
        .collect();

    retry_forever(ports.retry_delay, || {
        ports.pool_nodes.push_disk_states(states.clone())
    })
    .await;

    let commits: Vec<_> = inputs
        .iter()
        .map(|input| MemberDiskCommit {
            disk: input.disk.clone(),
            change: MemberDiskChange::SetIo(IoState::Down),
        })
        .collect();

    retry_commit_forever(&ports, commits.clone()).await;
    progress(inputs, commits)
}

pub(crate) async fn bring_online(
    ports: ActionPorts,
    inputs: Vec<DiskActionInput>,
) -> Vec<RequestCompletion> {
    let disks = inputs.iter().map(|input| input.disk.clone()).collect();
    let opened = match ports.pool_nodes.open_disks(disks).await {
        Ok(opened) => opened,
        Err(error) => return retry_all(inputs, MemberDiskError::PoolNodes(error.0)),
    };
    let mut opened = open_results(opened);
    let mut candidates = Vec::new();
    let mut completed = Vec::new();

    for input in inputs {
        if input.cancel.is_cancelled() {
            completed.push(cancelled(input.request));
            continue;
        }

        match opened.remove(&input.disk) {
            Some(Ok(())) => candidates.push(input),
            Some(Err(error)) => {
                completed.push(retry(input.request, MemberDiskError::PoolNodes(error.0)))
            }
            None => completed.push(failed(input.request, "open_disks omitted a requested disk")),
        }
    }

    if candidates.is_empty() {
        return completed;
    }

    let states: Vec<_> = candidates
        .iter()
        .map(|input| DiskStateChange {
            disk: input.disk.clone(),
            state: IoState::Up,
        })
        .collect();
    if let Err(error) = ports.pool_nodes.push_disk_states(states).await {
        completed.extend(retry_all(candidates, MemberDiskError::PoolNodes(error.0)));
        return completed;
    }

    let commits: Vec<_> = candidates
        .iter()
        .map(|input| MemberDiskCommit {
            disk: input.disk.clone(),
            change: MemberDiskChange::BringOnline,
        })
        .collect();
    completed.extend(commit_once(&ports, candidates, commits).await);
    completed
}

pub(crate) async fn disable_allocation(
    ports: ActionPorts,
    inputs: Vec<DiskActionInput>,
) -> Vec<RequestCompletion> {
    let commits = inputs
        .iter()
        .map(|input| MemberDiskCommit {
            disk: input.disk.clone(),
            change: MemberDiskChange::DisableAllocation,
        })
        .collect();
    commit_once(&ports, inputs, commits).await
}

pub(crate) async fn evacuate(ports: ActionPorts, input: DiskActionInput) -> Vec<RequestCompletion> {
    if input.cancel.is_cancelled() {
        return vec![cancelled(input.request)];
    }

    if let Err(error) = ports
        .virtual_disks
        .evacuate(&input.disk, &input.cancel)
        .await
    {
        return vec![if input.cancel.is_cancelled() {
            cancelled(input.request)
        } else {
            retry(input.request, MemberDiskError::VirtualDisks(error.0))
        }];
    }

    if input.cancel.is_cancelled() {
        return vec![cancelled(input.request)];
    }

    // Shrink may evacuate an online-but-inactive disk. DOWN publication is a
    // mandatory, idempotent safety boundary, so repeating it for offline disks
    // is deliberate.
    let state = DiskStateChange {
        disk: input.disk.clone(),
        state: IoState::Down,
    };
    retry_forever(ports.retry_delay, || {
        ports.pool_nodes.push_disk_states(vec![state.clone()])
    })
    .await;

    let commit = MemberDiskCommit {
        disk: input.disk.clone(),
        change: MemberDiskChange::Remove,
    };
    commit_once(&ports, vec![input], vec![commit]).await
}

pub(crate) async fn wait_recovery(input: WaitInput) -> Vec<RequestCompletion> {
    let deadline = input
        .observed_at
        .saturating_add(input.recovery_window.as_millis() as u64);
    let remaining = Duration::from_millis(deadline.saturating_sub(now_millis()));

    tokio::select! {
        _ = tokio::time::sleep(remaining) => {
            vec![RequestCompletion {
                request: input.request,
                result: StepResult::Progress(Vec::new()),
            }]
        }
        _ = input.cancel.cancelled() => vec![cancelled(input.request)],
    }
}

pub(crate) async fn allocate(ports: ActionPorts, plan: AllocationPlan) -> Vec<RequestCompletion> {
    commit_progress(&ports, plan.request, plan.commit).await
}

pub(crate) async fn release(ports: ActionPorts, plan: ReleasePlan) -> Vec<RequestCompletion> {
    commit_progress(&ports, plan.request, plan.commit).await
}

async fn commit_progress(
    ports: &ActionPorts,
    request: RequestId,
    commit: MemberDiskCommit,
) -> Vec<RequestCompletion> {
    let mut results = ports.metadata.commit(vec![commit.clone()]).await;
    if results.len() != 1 {
        return vec![failed(
            request,
            "metadata returned the wrong number of results",
        )];
    }

    match results.pop().expect("one metadata result was checked") {
        Ok(()) => vec![RequestCompletion {
            request,
            result: StepResult::Progress(committed(vec![commit])),
        }],
        Err(error) => vec![retry(request, MemberDiskError::Metadata(error.0))],
    }
}

async fn commit_once(
    ports: &ActionPorts,
    inputs: Vec<DiskActionInput>,
    commits: Vec<MemberDiskCommit>,
) -> Vec<RequestCompletion> {
    let results = ports.metadata.commit(commits.clone()).await;
    if results.len() != inputs.len() {
        return inputs
            .into_iter()
            .map(|input| {
                failed(
                    input.request,
                    "metadata returned the wrong number of results",
                )
            })
            .collect();
    }

    inputs
        .into_iter()
        .zip(commits)
        .zip(results)
        .map(|((input, commit), result)| match result {
            Ok(()) => RequestCompletion {
                request: input.request,
                result: StepResult::Progress(vec![CommittedChange {
                    disk: commit.disk,
                    change: commit.change,
                }]),
            },
            Err(error) => retry(input.request, MemberDiskError::Metadata(error.0)),
        })
        .collect()
}

async fn retry_commit_forever(ports: &ActionPorts, commits: Vec<MemberDiskCommit>) {
    loop {
        let results = ports.metadata.commit(commits.clone()).await;
        if results.len() == commits.len() && results.iter().all(Result::is_ok) {
            return;
        }
        tokio::time::sleep(ports.retry_delay).await;
    }
}

async fn retry_forever<F, Fut>(delay: Duration, mut operation: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), PortError>>,
{
    loop {
        if operation().await.is_ok() {
            return;
        }
        tokio::time::sleep(delay).await;
    }
}

fn progress(
    inputs: Vec<DiskActionInput>,
    commits: Vec<MemberDiskCommit>,
) -> Vec<RequestCompletion> {
    inputs
        .into_iter()
        .zip(commits)
        .map(|(input, commit)| RequestCompletion {
            request: input.request,
            result: StepResult::Progress(vec![CommittedChange {
                disk: commit.disk,
                change: commit.change,
            }]),
        })
        .collect()
}

fn committed(commits: Vec<MemberDiskCommit>) -> Vec<CommittedChange> {
    commits
        .into_iter()
        .map(|commit| CommittedChange {
            disk: commit.disk,
            change: commit.change,
        })
        .collect()
}

fn open_results(results: Vec<DiskOpenResult>) -> HashMap<DiskUuid, Result<(), PortError>> {
    results
        .into_iter()
        .map(|result| (result.disk, result.result))
        .collect()
}

fn retry_all(inputs: Vec<DiskActionInput>, error: MemberDiskError) -> Vec<RequestCompletion> {
    inputs
        .into_iter()
        .map(|input| retry(input.request, error.clone()))
        .collect()
}

fn retry(request: RequestId, error: MemberDiskError) -> RequestCompletion {
    RequestCompletion {
        request,
        result: StepResult::Retry(error),
    }
}

fn cancelled(request: RequestId) -> RequestCompletion {
    RequestCompletion {
        request,
        result: StepResult::Cancelled,
    }
}

fn failed(request: RequestId, reason: &str) -> RequestCompletion {
    RequestCompletion {
        request,
        result: StepResult::Failed(MemberDiskError::Internal(reason.into())),
    }
}
