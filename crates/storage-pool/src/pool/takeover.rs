//! Pool activation after the system's ownership/leader transition. User task controls
//! never perform this orchestration and never reconstruct a Workflow.
use super::*;
use crate::task_center::RecoveryReport;

impl PoolHandle {
    /// The system calls once after fencing the previous owner. Restores all known
    /// members using a fresh topology snapshot, then automatically recovers tasks.
    pub async fn takeover(&self, topology: Vec<TopologyEvent>) -> Outcome<RecoveryReport> {
        let pool = self.clone();
        let result = tokio::spawn(async move {
            let mut activated = pool.shared.takeover.lock().await;
            if *activated
                || (pool.task_center.has_task_store() && pool.task_center.recovery_initialized())
            {
                return Err(reject("Pool takeover is already initialized"));
            }
            pool.descriptor().active()?;
            pool.task_center.close_admission();
            pool.shared.descriptor.lock().unwrap().lifecycle = PoolLifecycle::Loading;
            let result = pool.activate(topology).await;
            match &result {
                Ok(report) => {
                    *activated = true;
                    *pool.shared.recovery_report.lock().unwrap() = Some(report.clone());
                }
                Err(error) => {
                    pool.shared.descriptor.lock().unwrap().lifecycle =
                        if matches!(error, Failure::Uncertain(_)) {
                            PoolLifecycle::ReconcileRequired
                        } else {
                            PoolLifecycle::Active
                        };
                    pool.task_center.close_admission();
                }
            }
            result
        })
        .await;
        result.unwrap_or_else(|_| {
            self.shared.descriptor.lock().unwrap().lifecycle = PoolLifecycle::ReconcileRequired;
            self.task_center.close_admission();
            Err(Failure::Uncertain(
                "Pool takeover finalizer terminated".into(),
            ))
        })
    }
    async fn activate(&self, topology: Vec<TopologyEvent>) -> Outcome<RecoveryReport> {
        let mut restore = self
            .task_center
            .submit_admin(RestoreMetadataWorkflow {
                domains: self.domains.clone(),
            })
            .await?;
        restore.result().await?;
        let mut found = BTreeSet::new();
        for event in &topology {
            let TopologyChange::NodeConnectivity { node, .. } = &event.change else {
                return Err(reject("takeover requires node connectivity snapshots"));
            };
            if event.id.is_nil() || !found.insert(node.clone()) {
                return Err(reject("duplicate node or invalid topology snapshot"));
            }
        }
        if !self.shared.nodes.lock().unwrap().is_subset(&found) {
            return Err(reject(
                "takeover snapshot does not cover restored Pool memberships",
            ));
        }
        let mut load = self
            .task_center
            .submit_admin(LoadPoolWorkflow {
                domains: self.domains.clone(),
                snapshot: topology,
                disks: self.disks().await?,
                attach_nodes: true,
            })
            .await?;
        load.result().await?;
        self.shared.descriptor.lock().unwrap().lifecycle = PoolLifecycle::Active;
        if self.task_center.has_task_store() {
            self.task_center.recover_tasks().await
        } else {
            self.task_center.open_admission();
            Ok(RecoveryReport::default())
        }
    }
    pub fn recovery_report(&self) -> Option<RecoveryReport> {
        self.shared.recovery_report.lock().unwrap().clone()
    }
}
