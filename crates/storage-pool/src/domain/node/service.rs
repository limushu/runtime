use crate::domain::{
    PoolId,
    node::{NodeId, PoolNode},
    object,
    ports::DataPlane,
    reject,
};
use async_trait::async_trait;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use workflow_foundation::Outcome;
use workflow_foundation::{ActivityContext, DomainService, ResourceAccess};

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
}
#[async_trait]
impl DomainService for NodeService {
    type Request = NodeCommand;
    type Reply = PoolNode;
    type Query = Option<NodeId>;
    type View = Vec<PoolNode>;
    fn resources(request: &NodeCommand) -> Vec<ResourceAccess> {
        let id = match request {
            NodeCommand::Inspect(id) => {
                return vec![ResourceAccess::read(object("node", id.as_str()))];
            }
            NodeCommand::Observe { id, .. } | NodeCommand::Load(id) => id,
        };
        vec![ResourceAccess::write(object("node", id.as_str()))]
    }
    async fn execute(&self, request: NodeCommand, ctx: ActivityContext) -> Outcome<PoolNode> {
        match request {
            NodeCommand::Inspect(id) => self.get(&id),
            NodeCommand::Observe {
                id,
                sequence,
                connected,
            } => {
                let mut nodes = self.nodes.lock().unwrap();
                if let Some(old) = nodes.get(&id) {
                    if sequence < old.observation {
                        return Ok(old.clone());
                    }
                    if sequence == old.observation {
                        return if old.connected == connected {
                            Ok(old.clone())
                        } else {
                            Err(reject("conflicting observation sequence"))
                        };
                    }
                }
                let loaded = nodes
                    .get(&id)
                    .is_some_and(|n| n.loaded && n.connected && connected);
                let node = PoolNode {
                    id: id.clone(),
                    pool: self.pool.clone(),
                    connected,
                    observation: sequence,
                    loaded,
                };
                nodes.insert(id, node.clone());
                Ok(node)
            }
            NodeCommand::Load(id) => {
                let mut node = self.get(&id)?;
                if !node.connected {
                    return Err(reject("cannot load a disconnected node"));
                }
                self.data_plane
                    .load_pool(&self.pool, &id, ctx.cancellation())
                    .await?;
                node.loaded = true;
                self.nodes.lock().unwrap().insert(id, node.clone());
                Ok(node)
            }
        }
    }
    fn query(&self, id: Option<NodeId>) -> Outcome<Vec<PoolNode>> {
        match id {
            Some(id) => Ok(vec![self.get(&id)?]),
            None => Ok(self.nodes.lock().unwrap().values().cloned().collect()),
        }
    }
}
