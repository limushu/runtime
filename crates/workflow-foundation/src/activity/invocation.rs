//! Bind an execute Future to the existing WorkflowContext without exposing it
//! to business code. Requests are driven serially by its owner; no context is
//! cloned, no detached executor is spawned, and native work retains its guards.
use super::*;
use crate::tasks::ActivityObservation;
use futures_util::future::BoxFuture;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};

type Operation = Box<
    dyn for<'a> FnOnce(
            &'a mut WorkflowContext,
            &'a ActivityObservation,
            Option<Failure>,
        ) -> BoxFuture<'a, Option<Failure>>
        + Send,
>;

struct Invocation {
    operations: mpsc::Sender<Operation>,
    id: ActivityId,
    name: &'static str,
    control: ControlToken,
    progress: watch::Sender<TaskProgress>,
    history: watch::Sender<ActivityHistory>,
}
tokio::task_local! {
    // Installed only while polling execute(), not in condition checks, result
    // handlers or service hosts. Tokio does not inherit this in spawned tasks.
    static CURRENT: Invocation;
}

fn current<T>(read: impl FnOnce(&Invocation) -> T) -> Outcome<T> {
    CURRENT
        .try_with(read)
        .map_err(|_| Failure::Rejected("operation requires Activity::execute()".into()))
}

pub(super) async fn execute<A: Activity>(
    context: &mut WorkflowContext,
    observation: &ActivityObservation,
    step: &mut A,
) -> Outcome<A::Reply> {
    let (operations, mut requests) = mpsc::channel::<Operation>(1);
    let invocation = Invocation {
        operations,
        id: observation.id(),
        name: observation.name(),
        control: context.control(),
        progress: context.progress.clone(),
        history: context.activities.clone(),
    };
    let body = CURRENT.scope(invocation, step.execute());
    tokio::pin!(body);
    let mut terminal_error = None;
    let result = loop {
        let operation = tokio::select! {
            biased;
            result = &mut body => break result,
            Some(operation) = requests.recv() => operation,
        };
        // Continue polling business code while its operation is pending: a
        // timeout/panic must still reach the existing settlement machinery.
        let terminal = tokio::select! {
            biased;
            result = &mut body => break result,
            terminal = operation(context, observation, terminal_error.clone()) => terminal,
        };
        if terminal.is_some() {
            terminal_error = terminal;
        }
    };
    match terminal_error {
        Some(error) => Err(error),
        None => result,
    }
}

async fn request<T: Send + 'static>(
    operation: impl for<'a> FnOnce(
        &'a mut WorkflowContext,
        &'a ActivityObservation,
    ) -> BoxFuture<'a, Outcome<T>>
    + Send
    + 'static,
) -> Outcome<T> {
    let sender = current(|invocation| invocation.operations.clone())?;
    let (mut reply, response) = oneshot::channel();
    let command: Operation = Box::new(move |context, observation, terminal| {
        Box::pin(async move {
            let result = if let Some(error) = terminal {
                Err(error)
            } else {
                tokio::select! {
                    biased;
                    // Abandoned queued requests never start; already dispatched
                    // work remains owned by its service guard until it settles.
                    _ = reply.closed() => return None,
                    result = operation(context, observation) => result,
                }
            };
            let terminal = match &result {
                Err(error @ (Failure::Cancelled | Failure::Uncertain(_))) => Some(error.clone()),
                _ => None,
            };
            let _ = reply.send(result);
            terminal
        })
    });
    sender
        .send(command)
        .await
        .map_err(|_| Failure::Unavailable)?;
    response.await.map_err(|_| Failure::Unavailable)?
}

pub(crate) async fn call<
    R: Send + 'static,
    P: Send + 'static,
    Q: Send + 'static,
    V: Send + 'static,
>(
    service: ActivityClient<R, P, Q, V>,
    value: R,
) -> Outcome<P> {
    request(move |context, observation| {
        Box::pin(async move {
            context
                .activity(&service, value)
                .parent(observation.id())
                .await
        })
    })
    .await
}

/// Park this execute Future after actual operations have settled. Resume checks
/// Task suspension reasons and the declared WF/Activity business conditions.
pub async fn wait_until_runnable() -> Outcome<()> {
    request(|context, observation| Box::pin(context.wait_observed(Some(observation)))).await
}

/// Cooperative local delay. Pause parks this same execute Future; cancellation
/// follows the Activity's policy. This does not stop external IO for the caller.
pub async fn sleep(delay: Duration) -> Outcome<()> {
    request(move |context, observation| Box::pin(context.sleep_observed(delay, Some(observation))))
        .await
}

/// The current logical step, available only inside Activity::execute().
pub fn id() -> Outcome<ActivityId> {
    current(|invocation| invocation.id)
}

/// Optional control access for local work. Service handlers receive their own
/// ActivityContext and do not need to use this function.
pub fn control() -> Outcome<ControlToken> {
    current(|invocation| invocation.control.clone())
}

/// Report progress with the declared step name. Invalid totals are ignored.
pub fn report_progress(completed: u64, total: Option<u64>) -> Outcome<()> {
    current(|invocation| {
        if total.is_some_and(|total| completed > total) {
            return;
        }
        let progress = TaskProgress {
            stage: Some(invocation.name.into()),
            completed,
            total,
        };
        invocation.progress.send_replace(progress.clone());
        invocation.history.send_modify(|history| {
            if let Some(entry) = history
                .entries
                .iter_mut()
                .find(|entry| entry.id == invocation.id)
            {
                entry.progress = progress;
            }
        });
    })
}
