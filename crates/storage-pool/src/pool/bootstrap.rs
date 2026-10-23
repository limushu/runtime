//! Construct services and the per-pool TaskCenter. No business steps run here.
use super::*;
impl Pool {
    pub(crate) fn start(
        descriptor: PoolDescriptor,
        config: Config,
        metadata: Arc<dyn MetadataStore>,
        data_plane: Arc<dyn DataPlane>,
        balancer: Arc<dyn Balancer>,
        registry: Weak<Mutex<crate::manager::Registry>>,
        task_store: Option<(
            Arc<dyn crate::task_center::persistence::TaskStore>,
            crate::task_center::persistence::PersistenceConfig,
        )>,
    ) -> Outcome<Self> {
        let id = descriptor.id.clone();
        let mut driver = WorkflowDriver::new(id.as_str(), config.clone())?;
        let tier = driver.register(
            "tier",
            TierHost(TierService::new(id.clone(), metadata.clone())),
        )?;
        let disk_service = Arc::new(DiskService::new(
            id.clone(),
            metadata.clone(),
            data_plane.clone(),
        ));
        let node_service = Arc::new(NodeService::new(id.clone(), data_plane.clone()));
        let disk = driver.register("disk", DiskHost(disk_service.clone()))?;
        let node = driver.register("node", NodeHost(node_service.clone()))?;
        let vd = driver.register(
            "vd",
            VdHost(VdService::new(id.clone(), metadata.clone(), data_plane)),
        )?;
        let balance_client = driver.register(
            "balance",
            crate::domains::balance::runtime::BalanceHost {
                pool: id.clone(),
                balancer: balancer.clone(),
            },
        )?;
        let shared = Arc::new(PoolShared::new(descriptor, registry, &config));
        let membership = driver.register(
            "membership",
            membership::MembershipHost {
                shared: shared.clone(),
                metadata: metadata.clone(),
                disks: disk_service,
                nodes: node_service,
            },
        )?;
        let domains = Domains {
            membership,
            balance: balance_client,
            disk,
            node,
            vd,
            tier,
        };
        let admin = driver.register(
            "pool",
            admin::AdminHost {
                shared: shared.clone(),
                domains: domains.clone(),
                metadata: metadata.clone(),
            },
        )?;
        let task_center = match task_store {
            Some((store, persistence_config)) => TaskCenter::with_task_store(
                driver,
                &config,
                shared.descriptor.lock().unwrap().uuid,
                shared.instance,
                store,
                persistence_config,
            )?,
            None => TaskCenter::start(driver, &config),
        };
        let handle = PoolHandle {
            metadata,
            balancer,
            id,
            task_center,
            domains,
            shared,
            admin,
        };
        handle.register_builtin_recovery()?;
        Ok(Self { handle })
    }
    pub fn handle(&self) -> PoolHandle {
        self.handle.clone()
    }
    pub async fn shutdown(&mut self) -> Outcome<()> {
        self.handle.shutdown().await
    }
}
