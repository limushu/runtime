#[allow(dead_code)]
mod support;
use async_trait::async_trait;
use std::{sync::Arc, time::Duration};
use support::*;
use workflow_foundation::{audit::*, *};

fn path() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("audit-test-{}.jsonl", uuid::Uuid::new_v4()))
}
async fn drive<T: Send + 'static>(driver: &mut WorkflowDriver, run: &mut Run<T>) -> Outcome<T> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            tokio::select! { result = run.result() => return result, _ = driver.tick() => {} }
        }
    })
    .await
    .expect("audit test stalled")
}
struct Steps(ServiceClient<Probe>);
struct LogicalStep(ServiceClient<Probe>);
#[async_trait]
impl Activity for LogicalStep {
    type Reply = usize;
    type Output = usize;
    fn name(&self) -> &'static str {
        "logical-step"
    }
    async fn execute(&mut self, scope: &mut ActivityScope<'_>) -> Outcome<usize> {
        scope.call(&self.0, Work::write("audit.call", "disk")).await
    }
    fn handle(&mut self, result: Outcome<usize>) -> Outcome<ActivityAction<usize>> {
        result.map(ActivityAction::Continue)
    }
}
#[async_trait]
impl Workflow for Steps {
    type Output = ();
    const NAME: &'static str = "test.audit";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        for _ in 0..4 {
            ctx.run_activity(LogicalStep(self.0.clone())).await?;
        }
        Ok(())
    }
}
#[tokio::test]
async fn durable_history_survives_memory_eviction_and_store_reopen() {
    let path = path();
    let store = Arc::new(FileAuditStore::open(&path).unwrap());
    let audit = AuditLog::new(store.clone(), 4096).unwrap();
    let mut driver = WorkflowDriver::new(
        "pool",
        Config {
            task_history: 0,
            activity_history: 1,
            ..Config::default()
        },
    )
    .unwrap()
    .with_audit(audit.clone())
    .unwrap();
    let client = driver
        .register(
            "probe",
            Probe::new(tokio::sync::mpsc::unbounded_channel().0),
        )
        .unwrap();
    let mut run = driver.submit(Steps(client)).unwrap();
    drive(&mut driver, &mut run).await.unwrap();
    driver.shutdown().await.unwrap();
    audit.flush().await.unwrap();
    let records = audit.records(0, 1000).unwrap();
    let completed: std::collections::BTreeSet<_> = records
        .iter()
        .filter(|r| {
            r.event.kind == "Activity"
                && r.event.data["state"] == "Completed"
                && r.event.data["kind"] == "Call"
        })
        .filter_map(|r| r.event.activity)
        .collect();
    assert_eq!(completed.len(), 4);
    let logical: std::collections::BTreeSet<_> = records
        .iter()
        .filter(|r| r.event.data["kind"] == "Step" && r.event.data["state"] == "Completed")
        .filter_map(|r| r.event.activity)
        .collect();
    assert_eq!(logical.len(), 4);
    assert!(
        records
            .iter()
            .filter(|r| r.event.data["kind"] == "Call")
            .all(|r| !r.event.data["parent_id"].is_null())
    );
    assert_eq!(run.control.activities().entries.len(), 1);
    assert_eq!(
        read_file(&path, 0, 1000).unwrap().len(),
        records.len(),
        "read-only access while writer is open"
    );
    assert!(
        FileAuditStore::open(&path).is_err(),
        "second writer rejected"
    );
    drop(run);
    drop(driver);
    drop(audit);
    drop(store);
    let reopened = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Ok(s) = FileAuditStore::open(&path) {
                break s;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(reopened.records(0, 1000).unwrap().len(), records.len());
}
struct Failing;
impl AuditStore for Failing {
    fn append(&self, _: AuditEvent) -> Outcome<()> {
        Err(Failure::Unavailable)
    }
    fn flush(&self) -> Outcome<()> {
        Err(Failure::Unavailable)
    }
    fn records(&self, _: u64, _: usize) -> Outcome<Vec<AuditRecord>> {
        Ok(vec![])
    }
}
#[tokio::test]
async fn audit_failure_prevents_execution_and_success_acknowledgement() {
    let audit = AuditLog::new(Arc::new(Failing), 32).unwrap();
    let mut driver = WorkflowDriver::new("pool", Config::default())
        .unwrap()
        .with_audit(audit)
        .unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let client = driver.register("probe", Probe::new(tx)).unwrap();
    let mut run = driver.submit(Steps(client)).unwrap();
    assert!(matches!(
        drive(&mut driver, &mut run).await,
        Err(Failure::Uncertain(_))
    ));
    assert!(rx.try_recv().is_err());
}
#[test]
fn partial_tail_is_ignored_and_recovered_but_corrupt_complete_frame_is_rejected() {
    use std::io::Write;
    let path = path();
    let store = FileAuditStore::open(&path).unwrap();
    drop(store);
    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"{partial")
        .unwrap();
    assert!(read_file(&path, 0, 10).unwrap().is_empty());
    drop(FileAuditStore::open(&path).unwrap());
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"corrupt\n")
        .unwrap();
    assert!(FileAuditStore::open(&path).is_err());
}

#[test]
fn a_new_epoch_fences_old_audit_writes_across_reopen() {
    let path = path();
    let event = |epoch| AuditEvent {
        schema: 1,
        task: None,
        pool_uuid: format!("{:032x}", 1),
        pool_name: "pool".into(),
        execution: "test".into(),
        owner_epoch: epoch,
        at_ms: 1,
        kind: "PoolRegistered".into(),
        activity: None,
        data: serde_json::Value::Null,
    };
    let store = FileAuditStore::open(&path).unwrap();
    store.append(event(1)).unwrap();
    store.append(event(2)).unwrap();
    assert!(store.append(event(1)).is_err());
    drop(store);
    let store = FileAuditStore::open(&path).unwrap();
    assert!(store.append(event(1)).is_err());
    assert_eq!(store.records(0, 10).unwrap().len(), 2);
}
