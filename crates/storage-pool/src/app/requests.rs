use crate::{DiskId, Outcome, VolumeId, domain::reject};
use std::collections::BTreeSet;

/// First vertical slice: one mirrored BG, one BLK per replica, distinct nodes.
#[derive(Debug, Clone)]
pub struct CreateVolume {
    pub id: VolumeId,
    pub disks: Vec<DiskId>,
}
impl CreateVolume {
    pub(crate) fn validate(&self) -> Outcome<()> {
        let unique: BTreeSet<_> = self.disks.iter().collect();
        if !(2..=3).contains(&self.disks.len()) || unique.len() != self.disks.len() {
            return Err(reject("a mirrored BG requires 2 or 3 distinct disks"));
        }
        Ok(())
    }
}

/// Monotonic topology sequence, shared with observe_node notifications.
#[derive(Debug, Clone)]
pub struct NodeTransitionRequest {
    pub node: crate::NodeId,
    pub sequence: u64,
}
