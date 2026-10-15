use crate::domains::{
    PoolId,
    node::{NodeId, PoolNode},
};
use crate::{ports::*, support::reject};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tokio_util::sync::CancellationToken;
use workflow_foundation::Outcome;

pub(crate) enum NodeCommand {
    Inspect(NodeId),
    Observe {
        id: NodeId,
        sequence: u64,
        connected: bool,
    },
    Load(NodeId),
}
pub(crate) struct NodeService {
    pool: PoolId,
    nodes: Mutex<BTreeMap<NodeId, PoolNode>>,
    data_plane: Arc<dyn DataPlane>,
}
impl NodeService {
    pub fn new(pool: PoolId, data_plane: Arc<dyn DataPlane>) -> Self {
        Self {
            pool,
            nodes: Mutex::new(BTreeMap::new()),
            data_plane,
        }
    }
    fn get(&self, id: &NodeId) -> Outcome<PoolNode> {
        self.nodes
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or_else(|| reject("node has not been observed"))
    }
    pub async fn execute(
        &self,
        request: NodeCommand,
        cancel: &CancellationToken,
    ) -> Outcome<PoolNode> {
        match request {
            NodeCommand::Inspect(id) => self.get(&id),
            NodeCommand::Observe {
                id,
                sequence,
                connected,
            } => {
                let mut nodes = self.nodes.lock().unwrap();
                let node = PoolNode::observe(
                    nodes.get(&id),
                    self.pool.clone(),
                    id.clone(),
                    sequence,
                    connected,
                )?;
                nodes.insert(id, node.clone());
                Ok(node)
            }
            NodeCommand::Load(id) => {
                let mut node = self.get(&id)?;
                node.require_connected()?;
                self.data_plane.load_pool(&self.pool, &id, cancel).await?;
                node.loaded = true;
                self.nodes.lock().unwrap().insert(id, node.clone());
                Ok(node)
            }
        }
    }
    pub fn query(&self, id: Option<NodeId>) -> Outcome<Vec<PoolNode>> {
        match id {
            Some(id) => Ok(vec![self.get(&id)?]),
            None => Ok(self.nodes.lock().unwrap().values().cloned().collect()),
        }
    }
}
