//! Simulated C operation with retained stop/completion callbacks.
use super::MemoryMetadata;
use crate::domains::balance::*;
use async_trait::async_trait;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::watch;
use workflow_foundation::{ActivityExit, Failure, Outcome};

pub struct SimulatedBalance {
    storage: Arc<MemoryMetadata>,
    submissions: AtomicUsize,
}
impl SimulatedBalance {
    pub fn new(storage: Arc<MemoryMetadata>) -> Self {
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
                    break storage
                        .evacuate(&request.pool, &request.disks)
                        .map(|()| ActivityExit::Completed(current));
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
