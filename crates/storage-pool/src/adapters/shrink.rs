//! Executable simulator for the real shrink workflow. No C or real disk IO.
use crate::domains::{balance::*, shrink::*};
use crate::{DiskId, PoolId};
use async_trait::async_trait;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use workflow_foundation::{ActivityExit, Failure, Outcome, Readiness};

#[derive(Default)]
struct DiskState {
    intent: bool,
    allocation_disabled: bool,
    evacuated: bool,
    io_disabled: bool,
    removed: bool,
}
#[derive(Default)]
pub struct SimulatedShrinkStorage {
    blocked: AtomicBool,
    disks: Mutex<BTreeMap<(PoolId, DiskId), DiskState>>,
    steps: Mutex<Vec<(ShrinkStage, Vec<DiskId>)>>,
}
impl SimulatedShrinkStorage {
    pub fn set_ready(&self, ready: bool) {
        self.blocked.store(!ready, Ordering::SeqCst);
    }
    pub fn steps(&self) -> Vec<(ShrinkStage, Vec<DiskId>)> {
        self.steps.lock().unwrap().clone()
    }
    pub fn evacuated(&self, pool: &PoolId, disks: &[DiskId]) {
        let mut state = self.disks.lock().unwrap();
        for disk in disks {
            state
                .entry((pool.clone(), disk.clone()))
                .or_default()
                .evacuated = true;
        }
    }
    fn apply(
        &self,
        pool: &PoolId,
        disks: &[DiskId],
        stage: ShrinkStage,
        cancel: &CancellationToken,
    ) -> Outcome<()> {
        if cancel.is_cancelled() {
            return Err(Failure::Cancelled);
        }
        let mut state = self.disks.lock().unwrap();
        for disk in disks {
            let d = state.entry((pool.clone(), disk.clone())).or_default();
            let valid = match stage {
                ShrinkStage::RecordIntent => {
                    d.intent = true;
                    true
                }
                ShrinkStage::DisableAllocation if d.intent => {
                    d.allocation_disabled = true;
                    true
                }
                ShrinkStage::VerifyEvacuated => d.evacuated && d.allocation_disabled,
                ShrinkStage::DisableIo if d.evacuated => {
                    d.io_disabled = true;
                    true
                }
                ShrinkStage::RemoveMembers if d.io_disabled && d.evacuated => {
                    d.removed = true;
                    true
                }
                _ => false,
            };
            if !valid {
                return Err(Failure::Rejected(format!(
                    "invalid simulated disk transition: {stage:?}"
                )));
            }
        }
        self.steps.lock().unwrap().push((stage, disks.to_vec()));
        Ok(())
    }
}
#[async_trait]
impl ShrinkStorage for SimulatedShrinkStorage {
    async fn readiness(&self, _: &PoolId, _: &[DiskId], _: ShrinkStage) -> Outcome<Readiness> {
        Ok(if self.blocked.load(Ordering::SeqCst) {
            Readiness::Blocked("placement is not ready".into())
        } else {
            Readiness::Ready
        })
    }
    async fn record_intent(&self, p: &PoolId, d: &[DiskId], c: &CancellationToken) -> Outcome<()> {
        self.apply(p, d, ShrinkStage::RecordIntent, c)
    }
    async fn disable_allocation(
        &self,
        p: &PoolId,
        d: &[DiskId],
        c: &CancellationToken,
    ) -> Outcome<()> {
        self.apply(p, d, ShrinkStage::DisableAllocation, c)
    }
    async fn verify_evacuated(
        &self,
        p: &PoolId,
        d: &[DiskId],
        c: &CancellationToken,
    ) -> Outcome<()> {
        self.apply(p, d, ShrinkStage::VerifyEvacuated, c)
    }
    async fn disable_io(&self, p: &PoolId, d: &[DiskId], c: &CancellationToken) -> Outcome<()> {
        self.apply(p, d, ShrinkStage::DisableIo, c)
    }
    async fn remove_members(&self, p: &PoolId, d: &[DiskId], c: &CancellationToken) -> Outcome<()> {
        self.apply(p, d, ShrinkStage::RemoveMembers, c)
    }
}

pub struct SimulatedBalance {
    storage: Arc<SimulatedShrinkStorage>,
    submissions: AtomicUsize,
}
impl SimulatedBalance {
    pub fn new(storage: Arc<SimulatedShrinkStorage>) -> Self {
        Self {
            storage,
            submissions: AtomicUsize::new(0),
        }
    }
    pub fn submissions(&self) -> usize {
        self.submissions.load(Ordering::SeqCst)
    }
}
#[derive(Clone, Copy)]
enum Command {
    Run,
    Suspend,
    Cancel,
}
struct Operation {
    command: watch::Sender<Command>,
    progress: watch::Receiver<MigrationProgress>,
    result: watch::Receiver<Option<Outcome<MigrationExit>>>,
}
#[async_trait]
impl BalanceOperation for Operation {
    async fn completion(&self) -> Outcome<MigrationExit> {
        let mut result = self.result.clone();
        loop {
            if let Some(done) = result.borrow_and_update().clone() {
                return done;
            }
            result
                .changed()
                .await
                .map_err(|_| Failure::Uncertain("simulated C worker disappeared".into()))?;
        }
    }
    async fn suspend(&self) -> Outcome<()> {
        self.command.send_replace(Command::Suspend);
        Ok(())
    }
    async fn cancel(&self) -> Outcome<()> {
        self.command.send_replace(Command::Cancel);
        Ok(())
    }
    fn progress(&self) -> watch::Receiver<MigrationProgress> {
        self.progress.clone()
    }
}
#[async_trait]
impl BalanceBackend for SimulatedBalance {
    async fn submit(&self, request: Migrate) -> Outcome<Arc<dyn BalanceOperation>> {
        self.submissions.fetch_add(1, Ordering::SeqCst);
        let (command, mut commands) = watch::channel(Command::Run);
        let initial = MigrationProgress {
            completed: request.checkpoint.progress.completed,
            total: 20,
        };
        let (progress, progress_rx) = watch::channel(initial.clone());
        let (result, result_rx) = watch::channel(None);
        let storage = self.storage.clone();
        tokio::spawn(async move {
            let mut current = initial;
            let done = loop {
                if current.completed == current.total {
                    storage.evacuated(&request.pool, &request.disks);
                    break Ok(ActivityExit::Completed(current));
                }
                let command = *commands.borrow_and_update();
                match command {
                    Command::Cancel => break Err(Failure::Cancelled),
                    Command::Suspend => {
                        break Ok(ActivityExit::Suspended(MigrationCheckpoint {
                            resume: serde_json::json!({"copied": current.completed}),
                            progress: current,
                        }));
                    }
                    Command::Run => {}
                }
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(15)) => {
                        current.completed += 1;
                        progress.send_replace(current.clone());
                    }
                    changed = commands.changed() => {
                        if changed.is_err() { break Err(Failure::Uncertain("control bridge dropped".into())); }
                    }
                }
            };
            // Simulated operation owns no external resources after this point.
            result.send_replace(Some(done));
        });
        Ok(Arc::new(Operation {
            command,
            progress: progress_rx,
            result: result_rx,
        }))
    }
}
