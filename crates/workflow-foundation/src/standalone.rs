//! Optional standalone host. Storage Pool embeds WorkflowDriver instead of using this.
use crate::{
    service::{self, ServiceOwner},
    *,
};
use std::collections::BTreeSet;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

// Optional standalone host for consumers without their own application event loop.
// Storage Pool uses WorkflowDriver directly and never constructs this wrapper.
type Command = Box<dyn FnOnce(&mut WorkflowDriver) + Send>;
#[derive(Clone)]
pub struct RuntimeHandle {
    view: ExecutionView,
    tx: mpsc::Sender<Command>,
}
impl std::ops::Deref for RuntimeHandle {
    type Target = ExecutionView;
    fn deref(&self) -> &Self::Target {
        &self.view
    }
}
impl RuntimeHandle {
    pub async fn submit<W: Workflow>(&self, workflow: W) -> Outcome<Run<W::Output>> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Box::new(move |driver| {
                let _ = tx.send(driver.submit(workflow));
            }))
            .await
            .map_err(|_| Failure::Unavailable)?;
        rx.await.unwrap_or(Err(Failure::Unavailable))
    }
    pub async fn defer<W: Workflow>(&self, workflow: W) -> Outcome<(Run<W::Output>, StartPermit)> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Box::new(move |driver| {
                let _ = tx.send(driver.defer(workflow));
            }))
            .await
            .map_err(|_| Failure::Unavailable)?;
        rx.await.unwrap_or(Err(Failure::Unavailable))
    }
}
pub struct WorkflowRuntime {
    handle: RuntimeHandle,
    stop: CancellationToken,
    task: Option<JoinHandle<Outcome<()>>>,
    services: Vec<ServiceOwner>,
    names: BTreeSet<String>,
    capacity: usize,
    failure: Option<Failure>,
}
impl WorkflowRuntime {
    pub fn start(pool: impl Into<String>, config: Config) -> Outcome<Self> {
        let mut engine = WorkflowDriver::new(pool, config.clone())?;
        let view = engine.view();
        let (tx, mut rx) = mpsc::channel::<Command>(config.capacity);
        let stop = CancellationToken::new();
        let stopping = stop.clone();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = stopping.cancelled() => break,
                    _ = engine.tick() => {}
                    command = rx.recv() => match command {
                        Some(command) => command(&mut engine),
                        None => break,
                    }
                }
            }
            engine.begin_shutdown();
            rx.close();
            while let Ok(command) = rx.try_recv() {
                command(&mut engine);
            }
            engine.shutdown().await
        });
        Ok(Self {
            handle: RuntimeHandle { view, tx },
            stop,
            task: Some(task),
            services: vec![],
            names: BTreeSet::new(),
            capacity: config.capacity,
            failure: None,
        })
    }
    pub fn client(&self) -> RuntimeHandle {
        self.handle.clone()
    }
    pub fn register<S: ServiceHandler>(
        &mut self,
        name: impl Into<String>,
        service: S,
    ) -> Outcome<ServiceClient<S>> {
        if self.handle.view.scope.is_closed() {
            return Err(Failure::Unavailable);
        }
        let name = name.into();
        if name.trim().is_empty() || !self.names.insert(name.clone()) {
            return Err(Failure::Rejected(
                "service name empty or already registered".into(),
            ));
        }
        let (client, owner) = service::spawn(
            service,
            name.into(),
            self.handle.view.scope.clone(),
            self.capacity,
        );
        self.services.push(owner);
        Ok(client)
    }
    pub async fn submit<W: Workflow>(&self, workflow: W) -> Outcome<Run<W::Output>> {
        self.handle.submit(workflow).await
    }
    pub async fn shutdown(&mut self) -> Outcome<()> {
        self.handle.view.scope.close();
        self.stop.cancel();
        if let Some(task) = &mut self.task {
            if let Err(error) = task
                .await
                .unwrap_or_else(|_| Err(Failure::Uncertain("execution host terminated".into())))
            {
                self.failure.get_or_insert(error);
            }
            self.task.take();
        }
        for service in &mut self.services {
            if let Err(error) = service.shutdown().await {
                self.failure.get_or_insert(error);
            }
        }
        self.failure.clone().map_or(Ok(()), Err)
    }
}
impl Drop for WorkflowRuntime {
    fn drop(&mut self) {
        self.handle.view.scope.close();
        self.stop.cancel();
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
