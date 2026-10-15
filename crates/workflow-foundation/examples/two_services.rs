//! cargo run --example two_services
//! No storage business: a workflow composes two independently hosted capabilities.
use async_trait::async_trait;
use std::{sync::Mutex, time::Duration};
use workflow_foundation::*;

struct Source;
#[async_trait]
impl ServiceHandler for Source {
    type Request = u64;
    type Reply = String;
    type Query = ();
    type View = &'static str;
    fn resources(id: &u64) -> Vec<ResourceAccess> {
        vec![ResourceAccess::read(ObjectKey::new(
            "source",
            id.to_string(),
        ))]
    }
    async fn execute(&self, id: u64, _: ActivityContext) -> Outcome<String> {
        Ok(format!("item-{id}"))
    }
    fn query(&self, _: ()) -> Outcome<Self::View> {
        Ok("source ready")
    }
}
#[derive(Default)]
struct Sink {
    values: Mutex<Vec<String>>,
}
#[async_trait]
impl ServiceHandler for Sink {
    type Request = String;
    type Reply = usize;
    type Query = ();
    type View = Vec<String>;
    fn resources(_: &String) -> Vec<ResourceAccess> {
        vec![ResourceAccess::write(ObjectKey::new("sink", "collection"))]
    }
    async fn execute(&self, value: String, _: ActivityContext) -> Outcome<usize> {
        if value.is_empty() {
            return Err(Failure::Failed("empty values are not allowed".into()));
        }
        // Short state access, not an async object-conflict queue.
        let mut values = self.values.lock().unwrap();
        values.push(value);
        Ok(values.len())
    }
    fn query(&self, _: ()) -> Outcome<Self::View> {
        Ok(self.values.lock().unwrap().clone())
    }
}

struct Transfer {
    item: u64,
    source: ServiceClient<Source>,
    sink: ServiceClient<Sink>,
}
#[async_trait]
impl Workflow for Transfer {
    type Output = usize;
    const NAME: &'static str = "transfer";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![ObjectKey::new("transfer", self.item.to_string())]
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<usize> {
        let value = ctx.activity(&self.source, self.item).await?;
        // Source resources are released; logical ownership of this transfer remains.
        let signal = ctx.next_signal().await?;
        if signal.name != "continue" {
            return Err(Failure::Rejected("expected continue signal".into()));
        }
        ctx.sleep(Duration::from_millis(1)).await?;
        ctx.activity(&self.sink, value).await
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Outcome<()> {
    let mut pool = WorkflowRuntime::start("pool-example", Config::default())?;
    let source = pool.register("source", Source)?;
    let sink = pool.register("sink", Sink::default())?;
    let mut run = pool
        .submit(Transfer {
            item: 7,
            source,
            sink: sink.clone(),
        })
        .await?;
    run.control.signal(Signal::new("continue", "")).await?;
    println!("workflow {:?}, output {}", run.id(), run.result().await?);
    println!("sink state: {:?}", sink.query(()).await?);
    println!("workflow state: {:?}", run.control.state());
    pool.shutdown().await?;
    println!(
        "coordinator: {:?}; sink: {:?}",
        pool.client().snapshot().lifecycle,
        sink.snapshot().lifecycle
    );
    Ok(())
}
