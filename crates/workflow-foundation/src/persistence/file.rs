//! Single-host reference journal. Shared/distributed storage must implement TaskStore separately.
use super::*;
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

#[derive(Serialize, Deserialize)]
enum Event {
    Claim {
        pool: u128,
        epoch: u64,
    },
    Save {
        task: Box<SavedTask>,
        previous: Option<u64>,
    },
}
impl Event {
    fn apply(&self, state: &mut StoreState) -> Outcome<()> {
        match self {
            Self::Claim { pool, epoch } => state.claim(*pool, *epoch),
            Self::Save { task, previous } => state.save(*task.clone(), *previous),
        }
    }
}
struct FileState {
    file: File,
    state: StoreState,
    poisoned: bool,
}
pub struct FileTaskStore {
    inner: Mutex<FileState>,
}
fn io(error: std::io::Error) -> Failure {
    Failure::Uncertain(format!("task journal IO: {error}"))
}
fn checksum(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325, |n, b| {
        (n ^ *b as u64).wrapping_mul(0x100000001b3)
    })
}
impl FileTaskStore {
    pub fn open(path: impl AsRef<Path>) -> Outcome<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(io)?;
        file.try_lock()
            .map_err(|e| Failure::Rejected(format!("task journal already in use: {e}")))?;
        let mut bytes = vec![];
        file.read_to_end(&mut bytes).map_err(io)?;
        let mut offset = 0;
        let mut state = StoreState::default();
        while bytes.len() - offset >= 12 {
            let len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
            if len > 16 * 1024 * 1024 {
                return Err(Failure::Uncertain(
                    "invalid task journal frame length".into(),
                ));
            }
            if bytes.len() - offset - 12 < len {
                break;
            }
            let hash = u64::from_le_bytes(bytes[offset + 4..offset + 12].try_into().unwrap());
            let payload = &bytes[offset + 12..offset + 12 + len];
            if checksum(payload) != hash {
                return Err(Failure::Uncertain("corrupt task journal frame".into()));
            }
            let event: Event =
                serde_json::from_slice(payload).map_err(|e| Failure::Uncertain(e.to_string()))?;
            event.apply(&mut state)?;
            offset += 12 + len;
        }
        if offset < bytes.len() {
            file.set_len(offset as u64).map_err(io)?;
            file.sync_all().map_err(io)?;
        }
        file.seek(SeekFrom::End(0)).map_err(io)?;
        Ok(Self {
            inner: Mutex::new(FileState {
                file,
                state,
                poisoned: false,
            }),
        })
    }
    fn append(&self, event: Event) -> Outcome<()> {
        let mut inner = self.inner.lock().unwrap();
        if inner.poisoned {
            return Err(Failure::Uncertain(
                "reopen failed journal before reuse".into(),
            ));
        }
        let mut next = inner.state.clone();
        event.apply(&mut next)?;
        let bytes = serde_json::to_vec(&event).map_err(|e| Failure::Rejected(e.to_string()))?;
        if bytes.len() > 16 * 1024 * 1024 {
            return Err(Failure::Rejected(
                "task journal record exceeds 16 MiB".into(),
            ));
        }
        let mut frame = Vec::with_capacity(bytes.len() + 12);
        frame.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        frame.extend_from_slice(&checksum(&bytes).to_le_bytes());
        frame.extend_from_slice(&bytes);
        if let Err(error) = inner
            .file
            .write_all(&frame)
            .and_then(|_| inner.file.sync_all())
        {
            inner.poisoned = true;
            return Err(io(error));
        }
        inner.state = next;
        Ok(())
    }
}
impl TaskStore for FileTaskStore {
    fn claim(&self, pool: u128, epoch: u64) -> Outcome<()> {
        self.append(Event::Claim { pool, epoch })
    }
    fn save(&self, task: SavedTask, previous: Option<u64>) -> Outcome<()> {
        self.append(Event::Save {
            task: Box::new(task),
            previous,
        })
    }
    fn list(&self, pool: u128) -> Outcome<Vec<SavedTask>> {
        let inner = self.inner.lock().unwrap();
        if inner.poisoned {
            return Err(Failure::Uncertain("journal requires reopen".into()));
        }
        Ok(inner
            .state
            .records
            .values()
            .filter(|r| r.id.pool == pool)
            .cloned()
            .collect())
    }
}
