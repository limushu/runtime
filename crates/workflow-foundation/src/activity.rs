//! An Activity describes one attempt; the runner owns retries and observation.
//! Its resource lease and observation cover the complete body, including backoff.
use crate::{
    driver::execution::{ActivityGuard, Tracker},
    model::Scope,
    *,
};
use async_trait::async_trait;
use futures_util::{FutureExt, future::BoxFuture};
use std::{
    future::Future,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, SystemTime},
};
use tokio_util::sync::CancellationToken;

mod managed;
pub use managed::{ActivityAction, ActivityWait, ManagedActivity};

/// A named business step. The same instance executes every retry in one
/// invocation. Its metadata and complete resource set are read before execution.
/// Mutable fields are local execution state, not persisted checkpoints.
#[async_trait]
pub trait Activity: Send + 'static {
    type Output: Send + 'static;

    fn name(&self) -> &'static str;
    fn description(&self) -> Option<&str> {
        None
    }
    fn resources(&self) -> Vec<ResourceAccess> {
        vec![]
    }
    fn retry(&self) -> Option<Retry> {
        None
    }
    /// Execute one attempt and interpret its complete result. Do not implement
    /// the retry loop here. In-flight operations must settle before returning;
    /// cancellation remains cooperative through the supplied scope.
    async fn execute(
        &mut self,
        scope: &mut ActivityScope,
    ) -> Outcome<ActivityDecision<Self::Output>>;
}

type Dispatch<'a, T> =
    Box<dyn FnOnce(ActivityContext, ActivityGuard) -> BoxFuture<'a, Outcome<T>> + Send + 'a>;
pub(crate) struct Invocation<'a, T> {
    pub scope: Arc<Scope>,
    pub service: String,
    pub name: &'static str,
    pub resources: Vec<ResourceAccess>,
    pub dispatch: Dispatch<'a, T>,
}
impl<T: Send + 'static> Invocation<'static, T> {
    pub fn body<F, Fut>(
        scope: Arc<Scope>,
        name: &'static str,
        resources: Vec<ResourceAccess>,
        body: F,
    ) -> Self
    where
        F: FnOnce(ActivityScope) -> Fut + Send + 'static,
        Fut: Future<Output = Outcome<T>> + Send + 'static,
    {
        let body_scope = scope.clone();
        let held = resources.clone();
        Self {
            scope,
            service: "workflow".into(),
            name,
            resources,
            dispatch: Box::new(move |context, guard| {
                let activity = ActivityScope {
                    context,
                    scope: body_scope,
                    resources: held,
                    calls: Tracker::default(),
                    last_error: None,
                };
                Box::pin(run_body(activity, guard, body))
            }),
        }
    }
}

/// The body owns its guard independently of the Workflow's response Future.
async fn run_body<T, F, Fut>(
    activity: ActivityScope,
    mut guard: ActivityGuard,
    body: F,
) -> Outcome<T>
where
    T: Send + 'static,
    F: FnOnce(ActivityScope) -> Fut + Send + 'static,
    Fut: Future<Output = Outcome<T>> + Send + 'static,
{
    let execution = tokio::spawn(async move {
        let cancel = activity.context.cancel.clone();
        let result = if cancel.is_cancelled() {
            Err(Failure::Cancelled)
        } else {
            guard.started();
            invoke(activity, body).await
        };
        guard.settled(&result);
        drop(guard);
        result
    });
    execution
        .await
        .unwrap_or_else(|_| Err(Failure::Uncertain("activity body host disappeared".into())))
}

/// Run one attempt and seal its service capability before another attempt starts.
async fn invoke<T, F, Fut>(activity: ActivityScope, body: F) -> Outcome<T>
where
    F: FnOnce(ActivityScope) -> Fut,
    Fut: Future<Output = Outcome<T>>,
{
    let calls = activity.calls.clone();
    let cancel = activity.context.cancel.clone();
    let result = AssertUnwindSafe(async move { body(activity).await })
        .catch_unwind()
        .await
        .unwrap_or_else(|_| Err(Failure::Uncertain("activity body panicked".into())));
    let unfinished = calls.close().is_err();
    let uncertain = calls.settle(&cancel).await;
    if unfinished || uncertain {
        Err(Failure::Uncertain(
            "activity body has an unconsumed or uncertain service result".into(),
        ))
    } else {
        result
    }
}

/// Execution capability for one Activity body. Calls share the declared resource
/// lease and ActivityId; retries do not create new Activity records. Not cloneable.
pub struct ActivityScope {
    context: ActivityContext,
    scope: Arc<Scope>,
    resources: Vec<ResourceAccess>,
    calls: Tracker,
    // Diagnostic cause of a Retry decision, distinct from the business reason.
    last_error: Option<Failure>,
}
impl ActivityScope {
    fn attempt_scope(&self) -> Self {
        Self {
            context: ActivityContext {
                id: self.id(),
                cancel: self.context.cancel.clone(),
                control: self.control(),
                progress: self.context.progress.clone(),
                protected_controls: self.context.protected_controls.clone(),
            },
            scope: self.scope.clone(),
            resources: self.resources.clone(),
            calls: Tracker::default(),
            last_error: None,
        }
    }
    pub fn id(&self) -> ActivityId {
        self.context.id
    }
    pub fn control(&self) -> ControlToken {
        self.context.control()
    }
    pub fn cancellation(&self) -> &CancellationToken {
        self.context.cancellation()
    }
    pub fn report_progress(&self, stage: impl Into<String>, completed: u64, total: Option<u64>) {
        if self.calls.ready().is_ok() {
            self.context.report_progress(stage, completed, total);
        }
    }
    /// Await a real service reply. All accesses must be covered by this body's
    /// declaration; no locks are reacquired, so retries cannot deadlock themselves.
    /// Pause is delivered to the service through its usual control token. Consume
    /// its actual result before deciding whether to retry or return Suspended.
    pub async fn call<
        R: Send + 'static,
        P: Send + 'static,
        Q: Send + 'static,
        V: Send + 'static,
    >(
        &mut self,
        service: &ActivityClient<R, P, Q, V>,
        request: R,
    ) -> Outcome<P> {
        self.calls.ready()?;
        if self.context.cancel.is_cancelled() || self.scope.is_closed() {
            return Err(Failure::Cancelled);
        }
        if !Arc::ptr_eq(&self.scope, &service.scope) {
            return Err(Failure::Rejected(
                "service belongs to another Pool instance".into(),
            ));
        }
        for required in service.resources(&request) {
            if !self.resources.iter().any(|held| {
                held.object == required.object
                    && (held.access == Access::Write || required.access == Access::Read)
            }) {
                return Err(Failure::Rejected(
                    "service call exceeds declared Activity resources".into(),
                ));
            }
        }
        let cancel = self.context.cancel.child_token();
        let mut control = self.control();
        control.cancel = cancel.clone();
        let result = service
            .dispatch(
                request,
                ActivityContext {
                    id: self.id(),
                    cancel,
                    control,
                    progress: self.context.progress.clone(),
                    protected_controls: self.context.protected_controls.clone(),
                },
                self.calls.register_call()?,
            )
            .await;
        self.last_error = result.as_ref().err().cloned();
        if result.as_ref().is_err_and(Failure::uncertain) {
            self.calls.mark_uncertain();
        }
        result
    }
    /// A side-effect-free control boundary between attempts. Suspended is a
    /// request for the BODY to return its checkpoint, never a Task pause ACK.
    pub fn check_control(&self) -> Outcome<ActivityExit<(), ()>> {
        self.calls.ready()?;
        if self.context.cancel.is_cancelled() || self.scope.is_closed() {
            Err(Failure::Cancelled)
        } else if self.context.control.request().paused() {
            Ok(ActivityExit::Suspended(()))
        } else {
            Ok(ActivityExit::Completed(()))
        }
    }
    /// Interruptible retry backoff. Only the timer is abandoned on pause; this
    /// method must not wrap an in-flight domain Future.
    pub async fn sleep(&mut self, delay: Duration) -> Outcome<ActivityExit<(), ()>> {
        let deadline = tokio::time::Instant::now() + delay;
        let mut control = self.control();
        loop {
            if self.check_control()? == ActivityExit::Suspended(()) {
                return Ok(ActivityExit::Suspended(()));
            }
            tokio::select! {
                biased;
                changed = control.changed() => { changed?; },
                _ = tokio::time::sleep_until(deadline) => return self.check_control(),
            }
        }
    }
}

/// Business interpretation of a settled result. Failure uses Err(Failure).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActivityDecision<T> {
    Complete(T),
    Retry { reason: String },
}

/// Retry budget and delay, applied only when the result handler requests Retry.
/// The limit includes the first attempt; a new invocation after suspension
/// starts a new budget. This configuration never classifies business results.
#[derive(Debug, Clone, Copy)]
pub struct Retry {
    delay: Duration,
    attempts: u32,
}
impl Retry {
    /// By default, allow at most three attempts.
    pub fn interval(delay: Duration) -> Self {
        Self { delay, attempts: 3 }
    }
    pub fn max_attempts(mut self, attempts: u32) -> Self {
        self.attempts = attempts;
        self
    }
}

// The two arities keep ordinary functions context-free. Functions that invoke
// managed services explicitly accept ActivityScope; there is no task-local state.
#[doc(hidden)]
pub trait ActivityFnFactory<Args, T>: Send + 'static {
    type Future: Future<Output = Outcome<T>> + Send + 'static;
    fn start(&mut self, activity: ActivityScope) -> Self::Future;
}
impl<T, F, Fut> ActivityFnFactory<(), T> for F
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = Outcome<T>> + Send + 'static,
{
    type Future = Fut;
    fn start(&mut self, _: ActivityScope) -> Fut {
        self()
    }
}
impl<T, F, Fut> ActivityFnFactory<(ActivityScope,), T> for F
where
    F: FnMut(ActivityScope) -> Fut + Send + 'static,
    Fut: Future<Output = Outcome<T>> + Send + 'static,
{
    type Future = Fut;
    fn start(&mut self, activity: ActivityScope) -> Fut {
        self(activity)
    }
}

type Attempt<T> = Box<dyn FnMut(ActivityScope) -> BoxFuture<'static, Outcome<T>> + Send>;
type ResultHandler<T, U> = Box<dyn FnMut(Outcome<T>) -> Outcome<ActivityDecision<U>> + Send>;

pub(crate) async fn run_step<A: Activity>(
    mut activity: ActivityScope,
    mut step: A,
    retry: Option<Retry>,
    history: tokio::sync::watch::Sender<ActivityHistory>,
) -> Outcome<ActivityExit<A::Output, ()>> {
    let mut attempt = 0;
    history.send_modify(|history| {
        if let Some(entry) = history
            .entries
            .iter_mut()
            .find(|entry| entry.id == activity.id())
        {
            entry.attempts = 0;
        }
    });
    loop {
        if activity.check_control()? == ActivityExit::Suspended(()) {
            return Ok(ActivityExit::Suspended(()));
        }
        attempt += 1;
        history.send_modify(|history| {
            if let Some(entry) = history
                .entries
                .iter_mut()
                .find(|entry| entry.id == activity.id())
            {
                entry.attempts = attempt;
                entry.retry_at = None;
            }
        });
        // Never race the function against a control signal and drop its Future.
        let current = &mut step;
        let (decision, last_error) = invoke(activity.attempt_scope(), |mut scope| async move {
            let decision = current.execute(&mut scope).await?;
            Ok((decision, scope.last_error))
        })
        .await?;
        let reason = match decision {
            ActivityDecision::Complete(value) => return Ok(ActivityExit::Completed(value)),
            ActivityDecision::Retry { reason } => reason,
        };
        history.send_modify(|history| {
            if let Some(entry) = history
                .entries
                .iter_mut()
                .find(|entry| entry.id == activity.id())
            {
                entry.last_error = last_error;
                entry.retry_reason = Some(reason.clone());
            }
        });
        let Some(policy) = retry else {
            return Err(Failure::Failed(format!(
                "activity requested retry without a retry policy: {reason}"
            )));
        };
        if attempt >= policy.attempts {
            return Err(Failure::Failed(format!(
                "activity retry limit exhausted after {attempt} attempts: {reason}"
            )));
        }
        history.send_modify(|history| {
            if let Some(entry) = history
                .entries
                .iter_mut()
                .find(|entry| entry.id == activity.id())
            {
                entry.retry_at = SystemTime::now().checked_add(policy.delay);
            }
        });
        if activity.sleep(policy.delay).await? == ActivityExit::Suspended(()) {
            return Ok(ActivityExit::Suspended(()));
        }
    }
}

/// Functions are adapters for Activity, sharing its runner and settlement rules.
struct FunctionActivity<T, U> {
    function: Attempt<T>,
    handler: ResultHandler<T, U>,
    options: Options,
}
#[async_trait]
impl<T: Send + 'static, U: Send + 'static> Activity for FunctionActivity<T, U> {
    type Output = U;
    fn name(&self) -> &'static str {
        self.options.name.unwrap_or("activity_fn")
    }
    fn description(&self) -> Option<&str> {
        self.options.description.as_deref()
    }
    fn resources(&self) -> Vec<ResourceAccess> {
        self.options.resources.clone()
    }
    fn retry(&self) -> Option<Retry> {
        self.options.retry
    }
    async fn execute(&mut self, scope: &mut ActivityScope) -> Outcome<ActivityDecision<U>> {
        let result = invoke(scope.attempt_scope(), &mut self.function).await;
        // Terminal results cannot be reclassified by a result handler.
        if let Err(error @ (Failure::Cancelled | Failure::Uncertain(_))) = result {
            return Err(error);
        }
        scope.last_error = result.as_ref().err().cloned();
        (self.handler)(result)
    }
}

#[derive(Default)]
pub(crate) struct Options {
    pub name: Option<&'static str>,
    pub description: Option<String>,
    pub uninterruptible: bool,
    pub resources: Vec<ResourceAccess>,
    pub retry: Option<Retry>,
}
impl Options {
    pub fn validate(&self) -> Outcome<()> {
        if self
            .name
            .is_some_and(|name| name.trim().is_empty() || name.len() > 256)
            || self
                .description
                .as_ref()
                .is_some_and(|text| text.len() > 4096)
            || self.retry.as_ref().is_some_and(|retry| {
                retry.attempts == 0
                    || tokio::time::Instant::now()
                        .checked_add(retry.delay)
                        .is_none()
            })
        {
            return Err(Failure::Rejected("invalid Activity options".into()));
        }
        Ok(())
    }
}

type Execute<'a, T> = Box<dyn FnOnce(Options) -> BoxFuture<'a, Outcome<T>> + Send + 'a>;
/// Lazy configuration shared by direct service calls and Activity bodies.
#[must_use = "an Activity does nothing until awaited"]
pub struct ActivityCall<'a, T> {
    execute: Option<Execute<'a, T>>,
    options: Options,
    future: Option<BoxFuture<'a, Outcome<T>>>,
}
impl<'a, T> ActivityCall<'a, T> {
    pub(crate) fn new(
        execute: impl FnOnce(Options) -> BoxFuture<'a, Outcome<T>> + Send + 'a,
    ) -> Self {
        Self {
            execute: Some(Box::new(execute)),
            options: Options::default(),
            future: None,
        }
    }
    fn configure(&mut self) -> &mut Options {
        assert!(
            self.future.is_none(),
            "configure an Activity before polling it"
        );
        &mut self.options
    }
    /// Static step metadata; dynamic business progress still uses report_progress.
    pub fn name(mut self, name: &'static str) -> Self {
        self.configure().name = Some(name);
        self
    }
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.configure().description = Some(description.into());
        self
    }
    /// Defer Task pause/cancel through the real result; host shutdown remains
    /// effective. All calls and backoff within a body inherit this same control.
    pub fn uninterruptible(mut self) -> Self {
        self.configure().uninterruptible = true;
        self
    }
}
impl<T> Unpin for ActivityCall<'_, T> {}
impl<T> Future for ActivityCall<'_, T> {
    type Output = Outcome<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.future.is_none() {
            this.future = Some(this.execute.take().expect("Activity already started")(
                std::mem::take(&mut this.options),
            ));
        }
        this.future.as_mut().unwrap().as_mut().poll(cx)
    }
}

/// A named function invocation. Suspension between attempts returns Suspended;
/// the Workflow consumes this result before acknowledging the Task's pause.
#[must_use = "an Activity does nothing until awaited"]
pub struct ActivityFn<'a, T, U = T> {
    context: Option<&'a mut WorkflowContext>,
    function: Option<Attempt<T>>,
    handler: Option<ResultHandler<T, U>>,
    options: Options,
    future: Option<BoxFuture<'a, Outcome<ActivityExit<U, ()>>>>,
}
impl<'a, T: Send + 'static> ActivityFn<'a, T> {
    pub(crate) fn new<F, Args>(context: &'a mut WorkflowContext, mut function: F) -> Self
    where
        F: ActivityFnFactory<Args, T>,
    {
        Self {
            context: Some(context),
            function: Some(Box::new(move |scope| Box::pin(function.start(scope)))),
            handler: Some(Box::new(|result| result.map(ActivityDecision::Complete))),
            options: Options::default(),
            future: None,
        }
    }
}
impl<'a, T, U> ActivityFn<'a, T, U> {
    fn configure(&mut self) -> &mut Options {
        assert!(
            self.future.is_none(),
            "configure an Activity before polling it"
        );
        &mut self.options
    }
    pub fn name(mut self, name: &'static str) -> Self {
        self.configure().name = Some(name);
        self
    }
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.configure().description = Some(description.into());
        self
    }
    /// Union of accesses made by the function, held across all retry attempts.
    pub fn resources(mut self, resources: Vec<ResourceAccess>) -> Self {
        self.configure().resources = resources;
        self
    }
    pub fn retry(mut self, retry: Retry) -> Self {
        self.configure().retry = Some(retry);
        self
    }
    /// Interpret the complete owned result, optionally changing the output type.
    /// Runs after the attempt and its domain calls have settled. Cancelled and
    /// Uncertain bypass this handler. Reconfiguring replaces the previous handler.
    pub fn handle_result<V>(
        mut self,
        handler: impl FnMut(Outcome<T>) -> Outcome<ActivityDecision<V>> + Send + 'static,
    ) -> ActivityFn<'a, T, V> {
        self.configure();
        ActivityFn {
            context: self.context,
            function: self.function,
            handler: Some(Box::new(handler)),
            options: self.options,
            future: None,
        }
    }
}
impl<T, U> Unpin for ActivityFn<'_, T, U> {}
impl<T: Send + 'static, U: Send + 'static> Future for ActivityFn<'_, T, U> {
    type Output = Outcome<ActivityExit<U, ()>>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.future.is_none() {
            let context = this.context.take().expect("Activity already started");
            let function = this.function.take().unwrap();
            let handler = this.handler.take().unwrap();
            let options = std::mem::take(&mut this.options);
            this.future = Some(Box::pin(context.run_activity(FunctionActivity {
                function,
                handler,
                options,
            })));
        }
        this.future.as_mut().unwrap().as_mut().poll(cx)
    }
}
