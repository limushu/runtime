//! Pool-owned lifecycle operations. Unload retains control-plane state; shutdown ends the host.
use super::*;
use crate::support::reject;
use std::collections::BTreeSet;

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
                let nodes = pool.shared.nodes.lock().unwrap().iter().cloned().collect();
                let mut run = pool
                    .task_center
                    .submit_admin(UnloadPoolWorkflow {
                        nodes,
                        domains: pool.domains.clone(),
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
                        snapshot,
                        disks,
                        attach_nodes: false,
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
