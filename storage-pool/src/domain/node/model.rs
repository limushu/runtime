use crate::domain::PoolId;
use workflow_foundation::{Failure, Outcome};

identity!(NodeId);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolNode {
    pub id: NodeId,
    pub pool: PoolId,
    pub connected: bool,
    pub observation: u64,
    /// Confirmed for the current connectivity epoch, never inferred from Connected.
    pub loaded: bool,
}
impl PoolNode {
    pub fn serving(&self) -> bool {
        self.connected && self.loaded
    }
}
