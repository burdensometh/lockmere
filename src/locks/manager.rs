use std::collections::{BTreeMap, BTreeSet};

use super::{Blocker, WaitEdge, WaitGraph};
use crate::{
    error::{Error, Result},
    rows::{ReadInterval, TxId},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    Shared,
    Exclusive,
}

impl LockMode {
    fn conflicts(self, other: Self) -> bool {
        self == Self::Exclusive || other == Self::Exclusive
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockRequest {
    pub transaction: TxId,
    pub interval: ReadInterval,
    pub mode: LockMode,
}

impl LockRequest {
    pub fn new(transaction: TxId, interval: ReadInterval, mode: LockMode) -> Self {
        Self {
            transaction,
            interval,
            mode,
        }
    }

    fn conflicts(&self, other: &Self) -> bool {
        self.transaction != other.transaction
            && self.mode.conflicts(other.mode)
            && self.interval.overlaps(&other.interval)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WaitTicket {
    pub transaction: TxId,
    pub serial: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Acquire {
    Granted,
    Waiting(WaitTicket),
    Deadlock(Vec<TxId>),
}

#[derive(Debug, Clone)]
struct QueueEntry {
    ticket: WaitTicket,
    request: LockRequest,
}

#[derive(Debug, Default)]
pub struct LockManager {
    granted: BTreeMap<String, Vec<LockRequest>>,
    queue: BTreeMap<u64, QueueEntry>,
    pending: BTreeMap<TxId, u64>,
    next_serial: u64,
}

impl LockManager {
    pub fn enqueue(&mut self, request: LockRequest) -> Result<Acquire> {
        if request.interval.is_empty() || self.already_holds(&request) {
            return Ok(Acquire::Granted);
        }
        if self.pending.contains_key(&request.transaction) {
            return Err(Error::TransactionBusy(request.transaction));
        }
        let serial = self
            .next_serial
            .checked_add(1)
            .ok_or(Error::Limit("wait ticket sequence"))?;
        if self.blockers(&request, serial).is_empty() {
            self.grant(request);
            return Ok(Acquire::Granted);
        }
        self.next_serial = serial;
        let ticket = WaitTicket {
            transaction: request.transaction,
            serial,
        };
        self.pending.insert(ticket.transaction, serial);
        self.queue.insert(serial, QueueEntry { request, ticket });
        if let Some(cycle) = self.graph().cycle_through(ticket.transaction) {
            self.remove_pending(ticket.transaction);
            return Ok(Acquire::Deadlock(cycle));
        }
        Ok(Acquire::Waiting(ticket))
    }

    pub fn claim(&mut self, ticket: WaitTicket) -> Result<Acquire> {
        let entry = self
            .queue
            .get(&ticket.serial)
            .filter(|entry| entry.ticket == ticket)
            .ok_or(Error::BadWaitTicket)?;
        if !self.blockers(&entry.request, ticket.serial).is_empty() {
            return Ok(Acquire::Waiting(ticket));
        }
        let request = self
            .remove_pending(ticket.transaction)
            .expect("queued transaction");
        self.grant(request);
        Ok(Acquire::Granted)
    }

    pub fn cancel(&mut self, ticket: WaitTicket) -> Result<()> {
        if self.pending.get(&ticket.transaction) != Some(&ticket.serial) {
            return Err(Error::BadWaitTicket);
        }
        self.remove_pending(ticket.transaction);
        Ok(())
    }

    pub fn release(&mut self, transaction: TxId) {
        self.remove_pending(transaction);
        self.granted.retain(|_, locks| {
            locks.retain(|lock| lock.transaction != transaction);
            !locks.is_empty()
        });
    }

    pub fn graph(&self) -> WaitGraph {
        let edges = self.queue.values().flat_map(|entry| {
            self.blockers(&entry.request, entry.ticket.serial)
                .into_iter()
                .map(move |(blocking, reason)| WaitEdge {
                    waiting: entry.ticket.transaction,
                    blocking,
                    interval: entry.request.interval.clone(),
                    reason,
                })
        });
        WaitGraph::new(edges.collect())
    }

    pub fn ready(&self) -> Vec<WaitTicket> {
        self.queue
            .values()
            .filter(|entry| {
                self.blockers(&entry.request, entry.ticket.serial)
                    .is_empty()
            })
            .map(|entry| entry.ticket)
            .collect()
    }

    pub fn held_count(&self) -> usize {
        self.granted.values().map(Vec::len).sum()
    }

    pub fn waiting_count(&self) -> usize {
        self.queue.len()
    }

    fn already_holds(&self, request: &LockRequest) -> bool {
        self.granted
            .get(&request.interval.table)
            .into_iter()
            .flatten()
            .any(|lock| {
                lock.transaction == request.transaction
                    && lock.interval == request.interval
                    && (lock.mode == request.mode || lock.mode == LockMode::Exclusive)
            })
    }

    fn blockers(&self, request: &LockRequest, serial: u64) -> BTreeSet<(TxId, Blocker)> {
        let mut blocked = BTreeSet::new();
        for lock in self
            .granted
            .get(&request.interval.table)
            .into_iter()
            .flatten()
        {
            if request.conflicts(lock) {
                blocked.insert((lock.transaction, Blocker::Granted));
            }
        }
        for (_, entry) in self.queue.range(..serial) {
            if request.conflicts(&entry.request) {
                blocked.insert((entry.ticket.transaction, Blocker::EarlierWaiter));
            }
        }
        blocked
    }

    fn grant(&mut self, request: LockRequest) {
        let locks = self
            .granted
            .entry(request.interval.table.clone())
            .or_default();
        if request.mode == LockMode::Exclusive {
            locks.retain(|lock| {
                lock.transaction != request.transaction || lock.interval != request.interval
            });
        }
        locks.push(request);
    }

    fn remove_pending(&mut self, transaction: TxId) -> Option<LockRequest> {
        let serial = self.pending.remove(&transaction)?;
        self.queue.remove(&serial).map(|entry| entry.request)
    }
}
