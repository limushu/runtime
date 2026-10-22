//! Pool-owned lifecycle operations. Unload retains control-plane state; shutdown ends the host.
use super::{fanout::EventDecision, *};
use crate::{
    domains::{disk::service::DiskCommand, node::service::NodeCommand},
    support::{object, reject},
    task_center::{OperationKind, PoolOperation, PoolWorkflow},
    workflows::Domains,
};
use async_trait::async_trait;
use std::{collections::BTreeSet, sync::atomic::Ordering};
use workflow_foundation::{ObjectKey, Workflow, WorkflowContext};

struct UnloadPoolWorkflow {
    domains: Domains,
    shared: Arc<PoolShared>,
}
impl PoolWorkflow for UnloadPoolWorkflow {
    fn operation(&self) -> PoolOperation {
        PoolOperation {
            kind: OperationKind::UnloadPool,
            objects: self.objects(),
        }
    }
}
#[async_trait]
impl Workflow for UnloadPoolWorkflow {
    type Output = ();
    const NAME: &'static str = "storage.unload-pool";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![object("pool", "lifecycle")]
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        let nodes: Vec<_> = self.shared.nodes.lock().unwrap().iter().cloned().collect();
        for node in nodes {
            ctx.activity(&self.domains.node, NodeCommand::Unload(node))
                .await?;
        }
        Ok(())
    }
}
struct LoadPoolWorkflow {
    domains: Domains,
    shared: Arc<PoolShared>,
    snapshot: Vec<TopologyEvent>,
    disks: Vec<MemberDisk>,
}
impl PoolWorkflow for LoadPoolWorkflow {
    fn operation(&self) -> PoolOperation {
        PoolOperation {
            kind: OperationKind::LoadPool,
            objects: self.objects(),
        }
    }
}
#[async_trait]
impl Workflow for LoadPoolWorkflow {
    type Output = ();
    const NAME: &'static str = "storage.load-pool";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![object("pool", "lifecycle")]
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        let mut connected = BTreeSet::new();
        for event in self.snapshot {
            let TopologyChange::NodeConnectivity {
                node,
                connected: is_connected,
            } = &event.change
            else {
                return Err(reject("load requires node connectivity snapshots"));
            };
            let sequence = self
                .shared
                .serial
                .fetch_add(1, Ordering::SeqCst)
                .max(event.sequence);
            self.shared
                .serial
                .fetch_max(sequence.saturating_add(1), Ordering::SeqCst);
            ctx.activity(
                &self.domains.node,
                NodeCommand::Observe {
                    id: node.clone(),
                    sequence,
                    connected: *is_connected,
                },
            )
            .await?;
            let descriptor = self.shared.descriptor.lock().unwrap().clone();
            let receipt = PoolEventReceipt::immediate(
                descriptor.uuid,
                self.shared.instance,
                event.id,
                EventDecision::Applied,
            );
            self.shared
                .events
                .lock()
                .unwrap()
                .versions
                .insert(event.key(), (Arc::new(event.clone()), receipt));
            if *is_connected {
                ctx.activity(&self.domains.node, NodeCommand::Load(node.clone()))
                    .await?;
                connected.insert(node.clone());
            }
        }
        for disk in self.disks {
            if disk.io == IoState::Up && connected.contains(&disk.node) {
                ctx.activity(&self.domains.disk, DiskCommand::Open(disk.id.clone()))
                    .await?;
                ctx.activity(&self.domains.disk, DiskCommand::SetIo(disk.id, IoState::Up))
                    .await?;
            }
        }
        Ok(())
    }
}
impl PoolHandle {
    /// Stop this Pool's data-plane access while retaining its local metadata and observers.
    pub async fn unload(&self) -> Outcome<()> {
        {
            let mut descriptor = self.shared.descriptor.lock().unwrap();
            descriptor.active()?;
            descriptor.lifecycle = PoolLifecycle::Unloading;
            self.task_center.close_admission();
        }
        let pool = self.clone();
        tokio::spawn(async move {
            let result = async {
                let mut run = pool
                    .task_center
                    .submit_admin(UnloadPoolWorkflow {
                        domains: pool.domains.clone(),
                        shared: pool.shared.clone(),
                    })
                    .await?;
                run.result().await
            }
            .await;
            pool.finish_lifecycle(&result, PoolLifecycle::Unloaded, PoolLifecycle::Active);
            result
        })
        .await
        .unwrap_or_else(|_| {
            Err(Failure::Uncertain(
                "pool unload finalizer terminated".into(),
            ))
        })
    }
    /// Caller supplies a complete current NodeMap snapshot; cached pre-unload connectivity
    /// alone cannot authorize loading or enabling IO after an observation gap.
    pub async fn load(&self, snapshot: Vec<TopologyEvent>) -> Outcome<()> {
        {
            let mut descriptor = self.shared.descriptor.lock().unwrap();
            if descriptor.lifecycle != PoolLifecycle::Unloaded || self.task_center.stopped() {
                return Err(reject(
                    "pool is not logically unloaded or its host has shut down",
                ));
            }
            let mut found = BTreeSet::new();
            let cache = self.shared.events.lock().unwrap();
            for event in &snapshot {
                let TopologyChange::NodeConnectivity { node, .. } = &event.change else {
                    return Err(reject("load requires node connectivity snapshots"));
                };
                if event.id.is_nil() || !found.insert(node.clone()) {
                    return Err(reject("duplicate node or invalid event id in snapshot"));
                }
                if let Some((previous, _)) = cache.versions.get(&event.key())
                    && (event.version() < previous.version()
                        || (event.version() == previous.version()
                            && event.change != previous.change))
                {
                    return Err(reject("stale or conflicting load snapshot"));
                }
            }
            if found != *self.shared.nodes.lock().unwrap() {
                return Err(reject(
                    "load snapshot must cover exactly this pool's node memberships",
                ));
            }
            descriptor.lifecycle = PoolLifecycle::Loading;
        }
        let pool = self.clone();
        tokio::spawn(async move {
            let result = async {
                let disks = pool.disks().await?;
                let mut run = pool
                    .task_center
                    .submit_admin(LoadPoolWorkflow {
                        domains: pool.domains.clone(),
                        shared: pool.shared.clone(),
                        snapshot,
                        disks,
                    })
                    .await?;
                run.result().await
            }
            .await;
            pool.finish_lifecycle(&result, PoolLifecycle::Active, PoolLifecycle::Unloaded);
            result
        })
        .await
        .unwrap_or_else(|_| Err(Failure::Uncertain("pool load finalizer terminated".into())))
    }
    fn finish_lifecycle(
        &self,
        result: &Outcome<()>,
        success: PoolLifecycle,
        known_failure: PoolLifecycle,
    ) {
        let state = match result {
            Ok(()) => success,
            Err(Failure::Uncertain(_)) => PoolLifecycle::ReconcileRequired,
            Err(_) => known_failure,
        };
        self.shared.descriptor.lock().unwrap().lifecycle = state;
        if state == PoolLifecycle::Active {
            self.task_center.open_admission();
        }
    }
}
