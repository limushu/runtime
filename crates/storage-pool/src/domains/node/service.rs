use crate::domains::{
    PoolId,
    node::{NodeId, PoolNode},
};
use crate::{ports::*, support::reject};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};
use tokio_util::sync::CancellationToken;
use workflow_foundation::Outcome;

pub(crate) enum NodeCommand {
    Attach {
        id: NodeId,
        sequence: u64,
        connected: bool,
    },
    Detach(NodeId),
    Unload(NodeId),
    Inspect(NodeId),
    Observe {
        id: NodeId,
        sequence: u64,
        connected: bool,
    },
    Load(NodeId),
}
pub(crate) struct NodeService {
    retired: Mutex<BTreeSet<NodeId>>,
    pool: PoolId,
    nodes: Mutex<BTreeMap<NodeId, PoolNode>>,
    data_plane: Arc<dyn DataPlane>,
}
impl NodeService {
    pub fn new(pool: PoolId, data_plane: Arc<dyn DataPlane>) -> Self {
        Self {
            retired: Mutex::default(),
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
        let attach = matches!(request, NodeCommand::Attach { .. });
        match request {
            NodeCommand::Unload(id) => {
                let mut node = self.get(&id)?;
                self.data_plane.unload_pool(&self.pool, &id, cancel).await?;
                node.loaded = false;
                self.nodes.lock().unwrap().insert(id, node.clone());
                Ok(node)
            }
            NodeCommand::Detach(id) => {
                let node = self.nodes.lock().unwrap().get(&id).cloned();
                if node.as_ref().is_some_and(|node| node.loaded) {
                    self.data_plane.unload_pool(&self.pool, &id, cancel).await?;
                }
                self.nodes.lock().unwrap().remove(&id);
                self.retired.lock().unwrap().insert(id.clone());
                Ok(node.unwrap_or(PoolNode {
                    id,
                    pool: self.pool.clone(),
                    connected: false,
                    observation: 0,
                    loaded: false,
                }))
            }
            NodeCommand::Inspect(id) => self.get(&id),
            NodeCommand::Observe {
                id,
                sequence,
                connected,
            }
            | NodeCommand::Attach {
                id,
                sequence,
                connected,
            } => {
                let mut nodes = self.nodes.lock().unwrap();
                if self.retired.lock().unwrap().contains(&id) {
                    return Err(reject("node membership identity is retired"));
                }
                if !attach && !nodes.contains_key(&id) {
                    return Err(reject("node is not a member of this pool"));
                }
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
