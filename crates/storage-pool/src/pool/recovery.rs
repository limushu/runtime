use crate::task_center::PersistentRun;
use crate::task_center::persistence::*;
use crate::{Outcome, PoolHandle, Run, WorkflowId};
use std::future::Future;
use workflow_foundation::Workflow;

impl PoolHandle {
    pub async fn submit_shrink_task<W: Workflow>(&self, workflow: W) -> Outcome<Run<W::Output>> {
        self.descriptor().active()?;
        self.task_center.submit_shrink(workflow).await
    }
    pub async fn submit_resumable_shrink_task<W: Workflow>(
        &self,
        workflow: W,
        resume: ResumeSpec,
    ) -> Outcome<PersistentRun<W::Output>> {
        self.descriptor().active()?;
        self.task_center
            .submit_resumable_shrink(workflow, resume)
            .await
    }
    /// Explicit extension for user long-running workflows; all object conflicts still apply.
    pub async fn submit_resumable_task<W: Workflow>(
        &self,
        workflow: W,
        resume: ResumeSpec,
    ) -> Outcome<PersistentRun<W::Output>> {
        self.descriptor().active()?;
        self.task_center.submit_resumable(workflow, resume).await
    }
    pub fn register_task_recovery<W, F, Fut>(
        &self,
        kind: impl Into<String>,
        version: u32,
        factory: F,
    ) -> Outcome<()>
    where
        W: Workflow,
        F: Fn(StoredTask) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Outcome<W>> + Send + 'static,
    {
        self.task_center.register_recovery(kind, version, factory)
    }
    pub async fn close_task_store(&self) -> Outcome<()> {
        self.task_center.close_task_store().await
    }
    pub fn persistent_task_id(&self, workflow: WorkflowId) -> Outcome<TaskId> {
        self.task_center.persistent_task_id(workflow)
    }
    pub async fn task_history(&self, query: TaskQuery) -> Outcome<TaskPage> {
        self.task_center.task_history(query).await
    }
    pub async fn task_detail(&self, id: TaskId) -> Outcome<StoredTask> {
        self.task_center.task_detail(id).await
    }
    pub async fn flush_task_history(&self) -> Outcome<()> {
        self.task_center.flush_history().await
    }
    pub fn task_persistence_health(&self) -> Outcome<PersistenceHealth> {
        self.task_center.persistence_health()
    }
    /// The caller must have completed the system's existing Pool takeover procedure first.
    pub async fn recover_tasks(&self) -> Outcome<Vec<StoredTask>> {
        self.task_center.recover_tasks().await
    }
    pub async fn continue_task(&self, id: TaskId) -> Outcome<WorkflowId> {
        self.descriptor().active()?;
        self.task_center.continue_task(id).await
    }
    pub async fn pause_task(&self, id: TaskId) -> Outcome<()> {
        self.task_center.pause_task(id).await
    }
    pub async fn resume_task(&self, id: TaskId) -> Outcome<WorkflowId> {
        self.descriptor().active()?;
        self.task_center.resume_task(id).await
    }
    pub async fn cancel_task(&self, id: TaskId) -> Outcome<()> {
        self.task_center.cancel_task(id).await
    }
}

impl PoolHandle {
    pub(super) fn register_builtin_recovery(&self) -> Outcome<()> {
        if !self.task_center.has_task_store() {
            return Ok(());
        }
        let domains = self.domains.clone();
        let metadata = self.metadata.clone();
        let balance = self.balancer.clone();
        let shared = self.shared.clone();
        self.register_task_recovery("storage.shrink-disk", 1, move |task| {
            let domains = domains.clone();
            let metadata = metadata.clone();
            let balance = balance.clone();
            let shared = shared.clone();
            async move {
                let resume = task
                    .resume
                    .ok_or_else(|| crate::support::reject("missing shrink resume input"))?;
                let request = serde_json::from_slice(&resume.input)
                    .map_err(|e| crate::support::reject(e.to_string()))?;
                Ok(super::shrink_disk::build_shrink(
                    domains, request, metadata, balance, shared,
                ))
            }
        })
    }
}
