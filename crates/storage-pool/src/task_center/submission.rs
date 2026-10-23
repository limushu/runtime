//! Typed submissions and optional audit bindings. Scheduling never performs database IO.
use super::{persistence::*, *};

pub(super) struct Accepted {
    pub control: RunControl,
    pub start: StartPermit,
    pub publish: Box<dyn FnOnce() + Send>,
}
type Accept = Box<dyn FnOnce(Outcome<&mut WorkflowDriver>) -> Option<Accepted> + Send>;
pub(super) struct Submission {
    pub task_id: TaskId,
    pub name: String,
    pub operation: PoolOperation,
    pub accept: Accept,
    pub administrative: bool,
    /// Attach an execution to a logical task already registered in TaskCenter.
    pub reconstructing: bool,
}
impl Submission {
    fn new<W: Workflow>(
        workflow: W,
        operation: PoolOperation,
        administrative: bool,
        binding: Option<TaskBinding>,
        reply: impl FnOnce(Outcome<TaskRun<W::Output>>) + Send + 'static,
    ) -> Self {
        let task_id = binding.as_ref().map(|b| b.id).unwrap_or_default();
        let reconstructing = binding.as_ref().is_some_and(|b| b.is_restored);
        let paused = binding.as_ref().is_some_and(|b| b.initial_pause);
        let options = binding.as_ref().map(|b| b.options()).unwrap_or_default();
        Self {
            task_id,
            name: W::NAME.into(),
            operation,
            administrative,
            reconstructing,
            accept: Box::new(move |driver| {
                let result = driver
                    .and_then(|d| d.defer_with_options(workflow, options))
                    .and_then(|(run, start)| {
                        if paused {
                            run.control.pause()?;
                        }
                        Ok((run, start))
                    });
                match result {
                    Err(error) => {
                        if let Some(b) = binding {
                            b.rejected(error.clone());
                        }
                        reply(Err(error));
                        None
                    }
                    Ok((run, start)) => Some(Accepted {
                        control: run.control.clone(),
                        start,
                        publish: Box::new(move || reply(Ok(TaskRun::new(task_id, run)))),
                    }),
                }
            }),
        }
    }
}
impl TaskCenter {
    /// Extension point for application-owned workflows. Uses the same conflict/object rules.
    pub async fn submit_task<W: Workflow>(&self, workflow: W) -> Outcome<TaskRun<W::Output>> {
        self.submit_classified(workflow, OperationKind::UserTask(W::NAME.into()))
            .await
    }
    /// Shrink participates in pool-wide topology coordination and retains its single slot
    /// while paused or waiting on business conditions. A read-only guard is required.
    pub async fn submit_shrink<W: Workflow>(&self, workflow: W) -> Outcome<TaskRun<W::Output>> {
        if workflow.continuation_guard().is_none() {
            return Err(Failure::Rejected(
                "shrink requires a continuation guard".into(),
            ));
        }
        self.submit_classified(workflow, OperationKind::Shrink)
            .await
    }
    pub async fn submit_topology<W: Workflow>(&self, workflow: W) -> Outcome<TaskRun<W::Output>> {
        self.submit_classified(workflow, OperationKind::Topology(W::NAME.into()))
            .await
    }
    pub async fn submit_operation<W: Workflow>(
        &self,
        workflow: W,
        operation: PoolOperation,
    ) -> Outcome<TaskRun<W::Output>> {
        if operation.objects != workflow.objects() {
            return Err(Failure::Rejected(
                "operation objects must match workflow objects".into(),
            ));
        }
        if operation.kind == OperationKind::Shrink && workflow.continuation_guard().is_none() {
            return Err(Failure::Rejected(
                "shrink requires a continuation guard".into(),
            ));
        }
        let binding = self.record_submission::<W>(&operation, None);
        self.submit_prepared(workflow, operation, false, binding)
            .await
    }
    async fn submit_classified<W: Workflow>(
        &self,
        workflow: W,
        kind: OperationKind,
    ) -> Outcome<TaskRun<W::Output>> {
        let operation = PoolOperation {
            kind,
            objects: workflow.objects(),
        };
        let binding = self.record_submission::<W>(&operation, None);
        self.submit_prepared(workflow, operation, false, binding)
            .await
    }
    pub async fn submit_resumable<W: Workflow>(
        &self,
        workflow: W,
        resume: ResumeSpec,
    ) -> Outcome<TaskRun<W::Output>> {
        self.submit_resumable_class(workflow, resume, TaskClass::General)
            .await
    }
    pub async fn submit_resumable_shrink<W: Workflow>(
        &self,
        workflow: W,
        resume: ResumeSpec,
    ) -> Outcome<TaskRun<W::Output>> {
        if workflow.continuation_guard().is_none() {
            return Err(Failure::Rejected(
                "shrink requires a continuation guard".into(),
            ));
        }
        self.submit_resumable_class(workflow, resume, TaskClass::Shrink)
            .await
    }
    async fn submit_resumable_class<W: Workflow>(
        &self,
        workflow: W,
        resume: ResumeSpec,
        class: TaskClass,
    ) -> Outcome<TaskRun<W::Output>> {
        if !self.accepts() {
            return Err(Failure::Unavailable);
        }
        let operation = PoolOperation {
            kind: if class == TaskClass::Shrink {
                OperationKind::Shrink
            } else {
                OperationKind::UserTask(W::NAME.into())
            },
            objects: workflow.objects(),
        };
        if operation.objects.is_empty() {
            return Err(Failure::Rejected(
                "resumable tasks must declare their objects".into(),
            ));
        }
        let binding = self
            .persistence()?
            .resumable_binding(
                TaskId::new(),
                W::NAME.into(),
                operation.objects.clone(),
                resume,
                class,
            )
            .await?;
        self.submit_prepared(workflow, operation, false, Some(binding))
            .await
    }

    pub(crate) async fn submit<W: PoolWorkflow>(&self, workflow: W) -> Outcome<TaskRun<W::Output>> {
        self.submit_with_access(workflow, false).await
    }
    pub(crate) async fn submit_admin<W: PoolWorkflow>(
        &self,
        workflow: W,
    ) -> Outcome<TaskRun<W::Output>> {
        self.submit_with_access(workflow, true).await
    }
    fn record_submission<W: Workflow>(
        &self,
        operation: &PoolOperation,
        source: Option<uuid::Uuid>,
    ) -> Option<TaskBinding> {
        self.persistence.as_ref().map(|p| {
            p.history_binding(
                TaskId::new(),
                W::NAME.into(),
                operation.objects.clone(),
                source,
                operation.kind.task_class(),
            )
        })
    }
    async fn submit_with_access<W: PoolWorkflow>(
        &self,
        workflow: W,
        administrative: bool,
    ) -> Outcome<TaskRun<W::Output>> {
        let operation = workflow.operation();
        let binding = self.record_submission::<W>(&operation, None);
        self.submit_prepared(workflow, operation, administrative, binding)
            .await
    }
    pub(super) async fn submit_prepared<W: Workflow>(
        &self,
        workflow: W,
        operation: PoolOperation,
        administrative: bool,
        binding: Option<TaskBinding>,
    ) -> Outcome<TaskRun<W::Output>> {
        if self.stop.is_cancelled() {
            if let Some(b) = binding {
                b.rejected(Failure::Unavailable);
            }
            return Err(Failure::Unavailable);
        }
        let failed_binding = binding.clone();
        let (reply, response) = oneshot::channel();
        let submission = Submission::new(
            workflow,
            operation,
            administrative,
            binding,
            move |result| {
                let _ = reply.send(result);
            },
        );
        if self.tx.send(submission).await.is_err() {
            if let Some(b) = failed_binding {
                b.rejected(Failure::Unavailable);
            }
            return Err(Failure::Unavailable);
        }
        response.await.unwrap_or(Err(Failure::Unavailable))
    }
    pub(crate) fn try_submit<W: PoolWorkflow>(
        &self,
        workflow: W,
        source_event: Option<uuid::Uuid>,
        callback: impl FnOnce(Outcome<TaskRun<W::Output>>) + Send + 'static,
    ) -> Outcome<()> {
        if !self.accepts() {
            return Err(Failure::Unavailable);
        }
        let operation = workflow.operation();
        let binding = self.record_submission::<W>(&operation, source_event);
        let failed_binding = binding.clone();
        self.tx
            .try_send(Submission::new(
                workflow, operation, false, binding, callback,
            ))
            .map_err(|e| {
                let failure = match e {
                    mpsc::error::TrySendError::Full(_) => Failure::Busy,
                    mpsc::error::TrySendError::Closed(_) => Failure::Unavailable,
                };
                if let Some(b) = failed_binding {
                    b.rejected(failure.clone());
                }
                failure
            })
    }
}
