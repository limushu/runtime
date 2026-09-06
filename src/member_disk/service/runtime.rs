use super::{
    MemberDiskClient, MemberDiskService, MemberDiskServiceError, client::ServiceMessage,
    reconcile::ReconcileResult,
};
use crate::member_disk::{DiskUuid, MemberDisk, MemberDiskEvent};
use futures_util::{StreamExt, stream::FuturesUnordered};
use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::Arc,
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

impl MemberDiskService {
    /// Starts the only Tokio task owned by this service. Per-disk steps are
    /// Futures polled by this task; they are not separately spawned tasks.
    pub fn spawn(self, queue_capacity: usize) -> (MemberDiskClient, JoinHandle<()>) {
        let (sender, receiver) = mpsc::channel(queue_capacity);
        let client = MemberDiskClient::new(sender);
        let service = Arc::new(self);
        let task = tokio::spawn(service.run(receiver));
        (client, task)
    }

    /// Owns event admission, one active event per disk, and all step Futures.
    async fn run(self: Arc<Self>, mut receiver: mpsc::Receiver<ServiceMessage>) {
        let mut pending_admissions = VecDeque::new();
        let mut admissions = FuturesUnordered::<AdmissionFuture>::new();
        let mut queries = FuturesUnordered::<QueryFuture>::new();
        let mut active: HashMap<DiskUuid, DiskSlot> = HashMap::new();
        let mut reconciles = FuturesUnordered::<ReconcileFuture>::new();
        let mut idle_waiters: HashMap<
            DiskUuid,
            Vec<oneshot::Sender<Result<(), MemberDiskServiceError>>>,
        > = HashMap::new();
        let mut last_results = HashMap::new();
        let mut accepting = true;

        while accepting
            || !pending_admissions.is_empty()
            || !admissions.is_empty()
            || !queries.is_empty()
            || !reconciles.is_empty()
        {
            tokio::select! {
                message = receiver.recv(), if accepting => match message {
                    Some(ServiceMessage::Submit { event, reply }) => {
                        pending_admissions.push_back(PendingAdmission { event, reply });
                        self.start_next_admission(&mut pending_admissions, &mut admissions);
                    }
                    Some(ServiceMessage::Get { disk, reply }) => {
                        let service = self.clone();
                        queries.push(Box::pin(async move {
                            let result = service.get_member(&disk).await;
                            QueryFinished { reply, result }
                        }));
                    }
                    Some(ServiceMessage::WaitIdle { disk, reply }) => {
                        if active.contains_key(&disk) {
                            idle_waiters.entry(disk).or_default().push(reply);
                        } else {
                            let result = last_results.get(&disk).cloned().unwrap_or(Ok(()));
                            let _ = reply.send(result);
                        }
                    }
                    None => {
                        // Dropping all clients drains admitted work. Force stop
                        // remains the caller's explicit JoinHandle::abort().
                        accepting = false;
                    }
                },
                Some(admission) = admissions.next(), if !admissions.is_empty() => {
                    match admission.result {
                        Ok(()) => {
                            self.admit_event(
                                admission.event,
                                &mut active,
                                &mut reconciles,
                                &mut last_results,
                            );
                            let _ = admission.reply.send(Ok(()));
                        }
                        Err(error) => {
                            let _ = admission.reply.send(Err(error));
                        }
                    }
                    self.start_next_admission(&mut pending_admissions, &mut admissions);
                }
                Some(query) = queries.next(), if !queries.is_empty() => {
                    let _ = query.reply.send(query.result);
                }
                Some(finished) = reconciles.next(), if !reconciles.is_empty() => {
                    let mut slot = active
                        .remove(&finished.disk)
                        .expect("a finished reconciliation must have an active disk slot");

                    if let Some(next) = slot.pending.pop_front() {
                        self.start_reconcile(
                            next,
                            slot.pending,
                            &mut active,
                            &mut reconciles,
                            &mut last_results,
                        );
                    } else {
                        match finished.result {
                            Ok(ReconcileResult::Transitioned) => self.start_reconcile(
                                slot.event,
                                VecDeque::new(),
                                &mut active,
                                &mut reconciles,
                                &mut last_results,
                            ),
                            Ok(ReconcileResult::Stable) => Self::finish_disk(
                                finished.disk,
                                Ok(()),
                                &mut idle_waiters,
                                &mut last_results,
                            ),
                            Err(error) => Self::finish_disk(
                                finished.disk,
                                Err(error),
                                &mut idle_waiters,
                                &mut last_results,
                            ),
                        }
                    }
                }
            }
        }
    }

    /// Event admission validates only that this Pool owns the disk. Physical
    /// facts stay on the event; no MemberDisk metadata is changed here.
    fn start_next_admission(
        self: &Arc<Self>,
        pending: &mut VecDeque<PendingAdmission>,
        admissions: &mut FuturesUnordered<AdmissionFuture>,
    ) {
        if !admissions.is_empty() {
            return;
        }
        let Some(pending) = pending.pop_front() else {
            return;
        };

        let service = self.clone();
        admissions.push(Box::pin(async move {
            let result = service.get_member(pending.event.disk()).await.map(|_| ());
            AdmissionFinished {
                event: pending.event,
                reply: pending.reply,
                result,
            }
        }));
    }

    /// Keeps one active event per disk. A distinct incoming event is queued and
    /// asks the current stable step to settle; adjacent duplicates are merged.
    fn admit_event(
        self: &Arc<Self>,
        event: MemberDiskEvent,
        active: &mut HashMap<DiskUuid, DiskSlot>,
        reconciles: &mut FuturesUnordered<ReconcileFuture>,
        last_results: &mut HashMap<DiskUuid, Result<(), MemberDiskServiceError>>,
    ) {
        let disk = event.disk().clone();
        let Some(slot) = active.get_mut(&disk) else {
            self.start_reconcile(event, VecDeque::new(), active, reconciles, last_results);
            return;
        };

        let duplicate = slot.pending.back().map_or_else(
            || slot.event.same_kind(&event),
            |last| last.same_kind(&event),
        );
        if duplicate {
            return;
        }

        slot.pending.push_back(event);
        slot.cancel.cancel();
    }

    fn start_reconcile(
        self: &Arc<Self>,
        event: MemberDiskEvent,
        pending: VecDeque<MemberDiskEvent>,
        active: &mut HashMap<DiskUuid, DiskSlot>,
        reconciles: &mut FuturesUnordered<ReconcileFuture>,
        last_results: &mut HashMap<DiskUuid, Result<(), MemberDiskServiceError>>,
    ) {
        let disk = event.disk().clone();
        let cancel = CancellationToken::new();

        // A later event is already waiting. The current event still gets one
        // chance to cross a mandatory boundary, while cancellable work settles
        // immediately and yields to the next event.
        if !pending.is_empty() {
            cancel.cancel();
        }

        last_results.remove(&disk);
        active.insert(
            disk.clone(),
            DiskSlot {
                event: event.clone(),
                cancel: cancel.clone(),
                pending,
            },
        );

        let service = self.clone();
        reconciles.push(Box::pin(async move {
            let result = service.reconcile_once(&event, &cancel).await;
            ReconcileFinished { disk, result }
        }));
    }

    fn finish_disk(
        disk: DiskUuid,
        result: Result<(), MemberDiskServiceError>,
        idle_waiters: &mut HashMap<
            DiskUuid,
            Vec<oneshot::Sender<Result<(), MemberDiskServiceError>>>,
        >,
        last_results: &mut HashMap<DiskUuid, Result<(), MemberDiskServiceError>>,
    ) {
        last_results.insert(disk.clone(), result.clone());
        if let Some(waiters) = idle_waiters.remove(&disk) {
            for waiter in waiters {
                let _ = waiter.send(result.clone());
            }
        }
    }
}

struct DiskSlot {
    event: MemberDiskEvent,
    cancel: CancellationToken,
    pending: VecDeque<MemberDiskEvent>,
}

struct PendingAdmission {
    event: MemberDiskEvent,
    reply: oneshot::Sender<Result<(), MemberDiskServiceError>>,
}

struct AdmissionFinished {
    event: MemberDiskEvent,
    reply: oneshot::Sender<Result<(), MemberDiskServiceError>>,
    result: Result<(), MemberDiskServiceError>,
}

struct QueryFinished {
    reply: oneshot::Sender<Result<MemberDisk, MemberDiskServiceError>>,
    result: Result<MemberDisk, MemberDiskServiceError>,
}

struct ReconcileFinished {
    disk: DiskUuid,
    result: Result<ReconcileResult, MemberDiskServiceError>,
}

type AdmissionFuture = Pin<Box<dyn Future<Output = AdmissionFinished> + Send>>;
type QueryFuture = Pin<Box<dyn Future<Output = QueryFinished> + Send>>;
type ReconcileFuture = Pin<Box<dyn Future<Output = ReconcileFinished> + Send>>;
