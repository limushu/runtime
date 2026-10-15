use crate::domains::PoolId;

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

impl PoolNode {
    pub(crate) fn observe(
        previous: Option<&Self>,
        pool: PoolId,
        id: NodeId,
        sequence: u64,
        connected: bool,
    ) -> crate::domains::DomainResult<Self> {
        if let Some(old) = previous {
            if sequence < old.observation {
                return Ok(old.clone());
            }
            if sequence == old.observation {
                return if old.connected == connected {
                    Ok(old.clone())
                } else {
                    Err(crate::domains::DomainError(
                        "conflicting observation sequence".into(),
                    ))
                };
            }
        }
        Ok(Self {
            id,
            pool,
            connected,
            observation: sequence,
            loaded: previous.is_some_and(|n| n.loaded && n.connected && connected),
        })
    }
    pub(crate) fn require_connected(&self) -> crate::domains::DomainResult<()> {
        if self.connected {
            Ok(())
        } else {
            Err(crate::domains::DomainError(
                "cannot load a disconnected node".into(),
            ))
        }
    }
}
