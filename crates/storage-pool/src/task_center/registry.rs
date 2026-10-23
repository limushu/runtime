//! TaskCenter's logical task catalog. Exists with or without a TaskStore.
use super::*;
use std::collections::VecDeque;
use workflow_foundation::{ExecutionObserver, ExecutionSnapshot};

pub(super) struct TaskRecord {
    pub id: TaskId,
    pub name: String,
    pub operation: PoolOperation,
    data: Mutex<RecordData>,
    changes: watch::Sender<u64>,
}
struct RecordData {
    status: TaskStatus,
    error: Option<Failure>,
    control: Option<RunControl>,
    observer: Option<ExecutionObserver>,
    conflicts: Vec<ConflictMatch>,
    waits: Vec<TaskWait>,
}
impl TaskRecord {
    pub fn new(
        id: TaskId,
        name: String,
        operation: PoolOperation,
        status: TaskStatus,
        error: Option<Failure>,
    ) -> Arc<Self> {
        let (changes, _) = watch::channel(0);
        Arc::new(Self {
            id,
            name,
            operation,
            changes,
            data: Mutex::new(RecordData {
                status,
                error,
                control: None,
                observer: None,
                conflicts: vec![],
                waits: vec![],
            }),
        })
    }
    fn notify(&self) {
        self.changes.send_modify(|revision| *revision += 1);
    }
    pub fn attach(
        &self,
        control: RunControl,
        observer: ExecutionObserver,
        conflicts: Vec<ConflictMatch>,
    ) {
        let mut data = self.data.lock().unwrap();
        data.control = Some(control);
        data.observer = Some(observer);
        data.conflicts = conflicts;
        data.error = None;
        drop(data);
        self.notify();
    }
    pub fn set_waits(&self, waits: Vec<TaskWait>) -> bool {
        let mut data = self.data.lock().unwrap();
        if data.waits == waits {
            return false;
        }
        data.waits = waits;
        drop(data);
        self.notify();
        true
    }
    pub fn cancel_inactive(&self) {
        let mut data = self.data.lock().unwrap();
        data.status = TaskStatus::Cancelled;
        data.waits.clear();
        drop(data);
        self.notify();
    }
    pub fn recovery_failed(&self, error: Failure) {
        self.data.lock().unwrap().error = Some(error);
        self.notify();
    }
    pub fn control(&self) -> Option<RunControl> {
        self.data.lock().unwrap().control.clone()
    }
    pub fn execution_observer(&self) -> Option<ExecutionObserver> {
        self.data.lock().unwrap().observer.clone()
    }
    pub fn snapshot(&self) -> TaskSnapshot {
        let data = self.data.lock().unwrap();
        let execution = data.observer.as_ref().map(ExecutionObserver::snapshot);
        let (status, error) = execution
            .as_ref()
            .map(status)
            .unwrap_or_else(|| (data.status.clone(), data.error.clone()));
        TaskSnapshot {
            id: self.id,
            name: self.name.clone(),
            operation: self.operation.clone(),
            status,
            last_error: error,
            execution,
            conflicts: data.conflicts.clone(),
            waits: data.waits.clone(),
        }
    }
    pub fn observe(self: &Arc<Self>) -> TaskObserver {
        // Subscribe first: an execution may attach while the observer is being
        // created. Reading first would lose that registration wakeup.
        let changes = self.changes.subscribe();
        let execution = self.execution_observer();
        let execution_id = execution.as_ref().map(|execution| execution.snapshot().id);
        TaskObserver {
            record: self.clone(),
            changes,
            execution,
            execution_id,
        }
    }
}
fn status(execution: &ExecutionSnapshot) -> (TaskStatus, Option<Failure>) {
    let status = match &execution.state {
        RunState::WaitingObjects => TaskStatus::Queued,
        RunState::Running
        | RunState::WaitingSignal
        | RunState::Pausing
        | RunState::WaitingResources
        | RunState::WaitingTimer
        | RunState::Activity(_)
        | RunState::Settling => TaskStatus::Running,
        RunState::Paused => TaskStatus::Paused,
        RunState::WaitingConditions(_) => TaskStatus::WaitingConditions,
        RunState::Completed => TaskStatus::Completed,
        RunState::Failed(Failure::Cancelled) => TaskStatus::Cancelled,
        RunState::Failed(Failure::Uncertain(_)) => TaskStatus::NeedsCheck,
        RunState::Failed(_) => TaskStatus::Failed,
    };
    let error = match &execution.state {
        RunState::Failed(error) => Some(error.clone()),
        _ => None,
    };
    (status, error)
}
#[derive(Default)]
pub(super) struct TaskRegistry {
    pub records: BTreeMap<TaskId, Arc<TaskRecord>>,
    completed: VecDeque<TaskId>,
}
impl TaskRegistry {
    pub fn get(&self, id: TaskId) -> Outcome<Arc<TaskRecord>> {
        self.records
            .get(&id)
            .cloned()
            .ok_or_else(|| crate::support::reject("unknown or expired task id"))
    }
    pub fn retain_history(&mut self, id: TaskId, limit: usize) {
        self.completed.push_back(id);
        while self.completed.len() > limit {
            if let Some(expired) = self.completed.pop_front() {
                self.records.remove(&expired);
            }
        }
    }
}
