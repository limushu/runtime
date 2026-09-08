use super::{
    MemberDiskService, MemberDiskServiceError,
    client::{Accepted, MemberDiskMessage},
    reconcile::ReconcileResult,
};
use crate::{
    member_disk::{DiskUuid, MemberDiskEvent, PhysicalState},
    runtime::{
        ConflictDecision, ManagedService, ObjectActivity, ObjectAdmission, ObjectLease,
        RequestContext, ServiceConfig, ServiceRuntime, TaskMeta, TaskOutcome,
    },
};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::oneshot;

impl MemberDiskService {
    pub fn spawn(self, queue_capacity: usize) -> super::MemberDiskRuntime {
        let mut config = ServiceConfig::new("member-disk", "member_disk");
        config.business_capacity = queue_capacity;
        self.spawn_with_config(config)
    }

    /// Starts this domain on the common runtime with explicit lifecycle,
    /// concurrency and observation configuration.
    pub fn spawn_with_config(self, config: ServiceConfig) -> super::MemberDiskRuntime {
        ServiceRuntime::spawn(self, config)
    }

    async fn handle_event(
        self: Arc<Self>,
        event: MemberDiskEvent,
        reply: oneshot::Sender<Result<Accepted, MemberDiskServiceError>>,
        context: RequestContext,
    ) -> Result<(), MemberDiskServiceError> {
        if let Err(error) = self.get_member(event.disk()).await.map(|_| ()) {
            let _ = reply.send(Err(error.clone()));
            return Err(error);
        }

        let disk = event.disk().clone();
        let admission =
            self.object_tasks
                .admit(disk, event, context.cancellation(), Self::resolve_conflict);
        let _ = reply.send(Ok(Accepted));

        let lease = match admission {
            ObjectAdmission::Joined => return Ok(()),
            ObjectAdmission::Active(lease) => lease,
            ObjectAdmission::Pending(pending) => pending
                .activate()
                .await
                .ok_or(MemberDiskServiceError::Cancelled)?,
        };
        self.run_object_workflow(lease, context).await
    }

    fn resolve_conflict(
        activity: ObjectActivity<'_, MemberDiskEvent>,
        incoming: &MemberDiskEvent,
    ) -> ConflictDecision {
        if activity.latest().same_kind(incoming) {
            return ConflictDecision::Join;
        }

        ConflictDecision::QueueAndCancel {
            cause: format!(
                "{} replaced by a newer {} event",
                event_kind(activity.active()),
                event_kind(incoming)
            ),
        }
    }

    async fn run_object_workflow(
        self: Arc<Self>,
        lease: ObjectLease<DiskUuid, MemberDiskEvent, MemberDiskServiceError>,
        context: RequestContext,
    ) -> Result<(), MemberDiskServiceError> {
        let event = lease.input().clone();
        let disk = event.disk().clone();
        let task = context.start_task(
            TaskMeta::new(
                format!("disk/{disk}"),
                event_kind(&event),
                format!("reconcile member disk {disk}"),
            ),
            lease.cancellation().clone(),
        );
        lease.bind_task(task.control());

        let result = loop {
            let before = self.reconcile_state(&disk).await?;
            task.blocked_on("next MemberDisk state transition");
            match self.reconcile_once(&event, task.cancellation()).await {
                Ok(ReconcileResult::Transitioned { action }) => {
                    task.unblocked();
                    let after = self.reconcile_state(&disk).await?;
                    task.transition(
                        format!("disk/{disk}"),
                        event_kind(&event),
                        format!("{before:?}"),
                        action,
                        format!("{after:?}"),
                    );
                    task.milestone(format!("member disk reached {after:?}"));
                }
                Ok(ReconcileResult::Stable) => {
                    task.unblocked();
                    break Ok(());
                }
                Err(error) => {
                    task.unblocked();
                    break Err(error);
                }
            }
        };

        let outcome = match &result {
            Ok(()) => TaskOutcome::Completed,
            Err(MemberDiskServiceError::Cancelled) => TaskOutcome::Cancelled,
            Err(error) => TaskOutcome::Failed(error.to_string()),
        };
        task.finish(outcome);
        lease.finish(result.clone());
        result
    }

    async fn handle_get(
        &self,
        disk: DiskUuid,
        reply: oneshot::Sender<Result<crate::member_disk::MemberDisk, MemberDiskServiceError>>,
    ) -> Result<(), MemberDiskServiceError> {
        let result = self.get_member(&disk).await;
        let status = result.as_ref().map(|_| ()).map_err(Clone::clone);
        let _ = reply.send(result);
        status
    }

    async fn handle_wait_idle(
        &self,
        disk: DiskUuid,
        reply: oneshot::Sender<Result<(), MemberDiskServiceError>>,
    ) -> Result<(), MemberDiskServiceError> {
        let result = self.object_tasks.wait_idle(disk).await;
        let status = result.clone();
        let _ = reply.send(result);
        status
    }

    async fn handle_allocation(
        &self,
        request: crate::member_disk::AllocateBlks,
        reply: oneshot::Sender<Result<crate::member_disk::Allocation, MemberDiskServiceError>>,
        context: RequestContext,
    ) -> Result<(), MemberDiskServiceError> {
        let task = context.start_task(
            TaskMeta::new(
                format!("tier/{}", request.tier()),
                "allocate_blks",
                format!("allocate {} BLKs", request.count()),
            )
            .non_cancellable(),
            context.cancellation().clone(),
        );
        task.blocked_on("SDB MemberDisk allocation commit");
        let result = self.allocate_blks(request).await;
        task.unblocked();
        if result.is_ok() {
            task.progress(100);
        }
        let status = result.as_ref().map(|_| ()).map_err(Clone::clone);
        let outcome = match &result {
            Ok(_) => TaskOutcome::Completed,
            Err(error) => TaskOutcome::Failed(error.to_string()),
        };
        task.finish(outcome);
        let _ = reply.send(result);
        status
    }
}

#[async_trait]
impl ManagedService for MemberDiskService {
    type Message = MemberDiskMessage;

    async fn handle(
        self: Arc<Self>,
        message: Self::Message,
        context: RequestContext,
    ) -> Result<(), MemberDiskServiceError> {
        match message {
            MemberDiskMessage::ApplyEvent { event, reply } => {
                self.handle_event(event, reply, context).await
            }
            MemberDiskMessage::Get { disk, reply } => self.handle_get(disk, reply).await,
            MemberDiskMessage::WaitIdle { disk, reply } => self.handle_wait_idle(disk, reply).await,
            MemberDiskMessage::Allocate { request, reply } => {
                self.handle_allocation(request, reply, context).await
            }
        }
    }
}

fn event_kind(event: &MemberDiskEvent) -> &'static str {
    match event {
        MemberDiskEvent::PhysicalChanged {
            state: PhysicalState::Down,
            ..
        } => "offline",
        MemberDiskEvent::PhysicalChanged {
            state: PhysicalState::Up,
            ..
        } => "online",
        MemberDiskEvent::Shrink { .. } => "shrink",
    }
}
