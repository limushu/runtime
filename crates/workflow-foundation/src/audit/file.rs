use super::*;
use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Seek, SeekFrom, Write},
    path::Path,
};

struct State {
    file: File,
    records: Vec<AuditRecord>,
    poisoned: bool,
    epochs: std::collections::BTreeMap<String, u64>,
}
pub struct FileAuditStore {
    inner: Mutex<State>,
    _writer_lock: File,
}
fn io(e: impl std::fmt::Display) -> Failure {
    Failure::Uncertain(format!("audit file: {e}"))
}
/// Read-only, including while the writer is running. An incomplete last frame is not a committed event.
pub fn read_file(path: impl AsRef<Path>, after: u64, limit: usize) -> Outcome<Vec<AuditRecord>> {
    let file = File::open(path).map_err(io)?;
    let mut reader = BufReader::new(file);
    let mut line = Vec::new();
    let mut records = Vec::new();
    let mut previous = 0;
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line).map_err(io)? == 0 {
            break;
        }
        if line.last() != Some(&b'\n') {
            break;
        }
        let record: AuditRecord = serde_json::from_slice(&line).map_err(io)?;
        if record.cursor != previous + 1 || record.event.schema != 1 {
            return Err(io("unsupported schema or non-contiguous cursor"));
        }
        previous = record.cursor;
        if record.cursor > after && records.len() < limit {
            records.push(record);
        }
    }
    Ok(records)
}
impl FileAuditStore {
    pub fn open(path: impl AsRef<Path>) -> Outcome<Self> {
        // Windows byte-range locks on the data file also deny read-only TUI access.
        // Fence writers using a separate lock file; readers never open it.
        let writer_lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.as_ref().with_extension("audit-writer.lock"))
            .map_err(io)?;
        writer_lock.try_lock().map_err(io)?;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.as_ref())
            .map_err(io)?;
        let records = read_file(path.as_ref(), 0, usize::MAX)?;
        let mut epochs = std::collections::BTreeMap::<String, u64>::new();
        for record in &records {
            let previous = epochs.entry(record.event.pool_uuid.clone()).or_default();
            if record.event.owner_epoch < *previous {
                return Err(io("stale epoch in journal"));
            }
            *previous = record.event.owner_epoch;
        }
        // Only truncate an unterminated tail; malformed complete records fail closed above.
        let mut reader = BufReader::new(file.try_clone().map_err(io)?);
        let mut line = Vec::new();
        let mut end = 0;
        while reader.read_until(b'\n', &mut line).map_err(io)? != 0 {
            if line.last() != Some(&b'\n') {
                break;
            }
            end += line.len() as u64;
            line.clear();
        }
        file.set_len(end).map_err(io)?;
        file.seek(SeekFrom::End(0)).map_err(io)?;
        file.sync_all().map_err(io)?;
        Ok(Self {
            inner: Mutex::new(State {
                file,
                records,
                poisoned: false,
                epochs,
            }),
            _writer_lock: writer_lock,
        })
    }
}
impl AuditStore for FileAuditStore {
    fn append(&self, event: AuditEvent) -> Outcome<()> {
        let mut state = self.inner.lock().unwrap();
        let pool = u128::from_str_radix(&event.pool_uuid, 16).map_err(io)?;
        if pool == 0
            || event.pool_uuid.len() != 32
            || event.schema != 1
            || event
                .task
                .is_some_and(|id| id.pool != pool || id.epoch > event.owner_epoch)
        {
            return Err(io("invalid audit scope or schema"));
        }
        if state
            .epochs
            .get(&event.pool_uuid)
            .is_some_and(|epoch| event.owner_epoch < *epoch)
        {
            return Err(Failure::Rejected("stale audit writer epoch".into()));
        }
        if state.poisoned {
            return Err(io("writer failed; reopen before reuse"));
        }
        let record = AuditRecord {
            cursor: state.records.len() as u64 + 1,
            event,
        };
        let mut bytes = serde_json::to_vec(&record).map_err(io)?;
        bytes.push(b'\n');
        if bytes.len() > 16 * 1024 * 1024 {
            return Err(io("record exceeds 16 MiB"));
        }
        if let Err(e) = state
            .file
            .write_all(&bytes)
            .and_then(|()| state.file.sync_data())
        {
            state.poisoned = true;
            return Err(io(e));
        }
        state
            .epochs
            .insert(record.event.pool_uuid.clone(), record.event.owner_epoch);
        state.records.push(record);
        Ok(())
    }
    fn flush(&self) -> Outcome<()> {
        let state = self.inner.lock().unwrap();
        if state.poisoned {
            return Err(io("writer failed"));
        }
        state.file.sync_all().map_err(io)
    }
    fn records(&self, after: u64, limit: usize) -> Outcome<Vec<AuditRecord>> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .records
            .iter()
            .filter(|r| r.cursor > after)
            .take(limit)
            .cloned()
            .collect())
    }
}
