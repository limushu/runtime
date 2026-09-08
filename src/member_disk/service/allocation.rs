use super::{MemberDiskService, MemberDiskServiceError};
use crate::member_disk::{AllocateBlks, Allocation, BlkRef};
use std::collections::HashSet;

impl MemberDiskService {
    /// Allocates one BLK from each selected MemberDisk in the requested Tier.
    ///
    /// The first version deliberately serializes this SDB-first transition
    /// with every other MemberDisk mutation through `mutation_gate`. Object
    /// guards are released before `.await`; a later Partition scheduler can
    /// narrow this gate without changing the public request API.
    pub(super) async fn allocate_blks(
        &self,
        request: AllocateBlks,
    ) -> Result<Allocation, MemberDiskServiceError> {
        if request.count() == 0 {
            return Err(MemberDiskServiceError::EmptyAllocation);
        }

        let _mutation = self.mutation_gate.lock().await;
        let plan = {
            let disks = self.disks.lock().await;
            let mut candidates: Vec<_> = disks
                .values()
                .filter(|disk| {
                    disk.tier_id() == request.tier()
                        && disk.can_allocate()
                        && !self.object_tasks.is_active(disk.uuid())
                })
                .collect();
            candidates.sort_by(|left, right| left.uuid().as_str().cmp(right.uuid().as_str()));

            let mut selected_domains = HashSet::new();
            let mut plan = Vec::with_capacity(request.count());

            for disk in candidates {
                let domain = if let Some(kind) = request.fault_domain() {
                    let Some(domain) = disk
                        .failure_domains()
                        .iter()
                        .find(|domain| domain.kind == kind)
                    else {
                        continue;
                    };
                    if selected_domains.contains(&domain.id) {
                        continue;
                    }
                    Some(domain.id.clone())
                } else {
                    None
                };

                let Ok(blk) = disk.plan_blk_allocation() else {
                    continue;
                };
                if let Some(domain) = domain {
                    selected_domains.insert(domain);
                }
                plan.push(BlkRef::new(disk.uuid().clone(), blk));
                if plan.len() == request.count() {
                    break;
                }
            }
            plan
        };

        if plan.len() != request.count() {
            return Err(MemberDiskServiceError::InsufficientAllocationCandidates {
                tier: request.tier().to_owned(),
                requested: request.count(),
                available: plan.len(),
            });
        }

        self.metadata.allocate_blks(&plan).await?;
        let mut disks = self.disks.lock().await;
        for allocated in &plan {
            disks
                .get_mut(allocated.disk())
                .expect("an allocation candidate must remain in the locked directory")
                .apply_committed_allocation(allocated.blk());
        }

        Ok(Allocation::new(plan))
    }
}
