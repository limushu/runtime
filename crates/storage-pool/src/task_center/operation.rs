//! Task classification and object claims. No Pool membership or entity state.
use super::*;
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperationKind {
    ObserveNode {
        id: NodeId,
        sequence: u64,
        connected: bool,
    },
    LoadNode(NodeId),
    OfflineNode {
        id: NodeId,
        sequence: u64,
    },
    OnlineNode {
        id: NodeId,
        sequence: u64,
    },
    OnlineDisk(DiskId),
    OfflineDisk(DiskId),
    CreateVolume(VolumeId),
    AttachNode(NodeId),
    DetachNode(NodeId),
    CreateTier(TierId),
    DeleteTier(TierId),
    RemoveDisk(DiskId),
    DeleteVolume(VolumeId),
    DeletePool,
    RenamePool,
    UnloadPool,
    LoadPool,
    UserTask(String),
    /// One unfinished shrink per Pool, including suspension and recovery.
    Shrink,
    /// Explicit application extension for topology workflows.
    Topology(String),
}
#[derive(Debug, Clone)]
pub struct PoolOperation {
    pub kind: OperationKind,
    pub objects: Vec<ObjectKey>,
}
impl OperationKind {
    pub(super) fn task_class(&self) -> persistence::TaskClass {
        match self {
            Self::Shrink => persistence::TaskClass::Shrink,
            kind if kind.is_topology() => persistence::TaskClass::Topology,
            _ => persistence::TaskClass::General,
        }
    }
    pub(super) fn from_task(task: &persistence::StoredTask) -> Self {
        match task.class {
            persistence::TaskClass::General => Self::UserTask(task.name.clone()),
            persistence::TaskClass::Shrink => Self::Shrink,
            persistence::TaskClass::Topology => Self::Topology(task.name.clone()),
        }
    }
}

pub(crate) trait PoolWorkflow: Workflow {
    fn operation(&self) -> PoolOperation;
}
