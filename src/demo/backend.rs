use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::time::sleep;

use crate::TaskContext;

use super::{DemoError, DiskId};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BackendSnapshot {
    pub started: Vec<DiskId>,
    pub cancel_requested: Vec<DiskId>,
    pub cancelled: Vec<DiskId>,
    pub completed: Vec<DiskId>,
}

#[derive(Clone)]
pub struct BgBackend {
    work_time: Duration,
    cancel_time: Duration,
    state: Arc<Mutex<BackendSnapshot>>,
}

impl BgBackend {
    pub fn new(work_time: Duration, cancel_time: Duration) -> Self {
        Self {
            work_time,
            cancel_time,
            state: Arc::new(Mutex::new(BackendSnapshot::default())),
        }
    }

    pub fn snapshot(&self) -> BackendSnapshot {
        self.state.lock().expect("backend state poisoned").clone()
    }

    pub async fn rebuild(&self, task: &TaskContext, disk: DiskId) -> Result<(), DemoError> {
        self.state
            .lock()
            .expect("backend state poisoned")
            .started
            .push(disk.clone());

        tokio::select! {
            biased;
            _reason = task.cancelled() => {
                self.state
                    .lock()
                    .expect("backend state poisoned")
                    .cancel_requested
                    .push(disk.clone());
                sleep(self.cancel_time).await;
                self.state
                    .lock()
                    .expect("backend state poisoned")
                    .cancelled
                    .push(disk);
                Err(DemoError::Cancelled)
            }
            () = sleep(self.work_time) => {
                self.state
                    .lock()
                    .expect("backend state poisoned")
                    .completed
                    .push(disk);
                Ok(())
            }
        }
    }
}
