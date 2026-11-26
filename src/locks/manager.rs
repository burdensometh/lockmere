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

/// A transaction chosen to break a wait cycle, together with the cycle that
/// justified the choice. The cycle starts and ends at the victim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadlockVictim {
    pub victim: TxId,
    pub cycle: Vec<TxId>,
}

/// What happened to the request that discovered a wait cycle once every victim
/// had been removed from the lock tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Requester {
    /// The requester's lock is now granted; the caller should run the request.
    Granted,
    /// The requester itself carried the lowest priority in a cycle and must
    /// abort.
    Victim,
    /// The requester survives but is still queued behind earlier waiters. A
    /// later claim resumes it with its original operation.
    Queued(WaitTicket),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Acquire {
    Granted,
    Waiting(WaitTicket),
    Deadlock {
        victims: Vec<DeadlockVictim>,
        requester: Requester,
    },
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
    priorities: BTreeMap<TxId, i64>,
    next_serial: u64,
}

impl LockManager {
    pub fn enqueue(&mut self, request: LockRequest) -> Result<Acquire> {
        self.enqueue_with_priority(request, 0)
    }

    /// Enqueues a request whose transaction has the given priority. Higher
    /// values are preserved when a wait cycle needs a victim; the default is
    /// zero. A later call for the same transaction replaces the stored value.
    pub fn enqueue_with_priority(
        &mut self,
        request: LockRequest,
        priority: i64,
    ) -> Result<Acquire> {
        self.priorities.insert(request.transaction, priority);
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
        let mut victims = Vec::new();
        loop {
            let Some(cycle) = self.graph().cycle_through(ticket.transaction) else {
                break;
            };
            let victim = self.select_victim(&cycle);
            // Drop both the waiting entry and any granted locks so a later
            // cycle search observes the state the abort will leave behind.
            self.release(victim);
            victims.push(DeadlockVictim {
                victim,
                cycle: cycle_from(&cycle, victim),
            });
            if victim == ticket.transaction {
                return Ok(Acquire::Deadlock {
                    victims,
                    requester: Requester::Victim,
                });
            }
        }
        if victims.is_empty() {
            return Ok(Acquire::Waiting(ticket));
        }
        // The requester survives. If the freed locks unblock it, grant now;
        // otherwise keep it queued so an earlier waiter is not overtaken.
        let ready = self
            .queue
            .get(&ticket.serial)
            .is_some_and(|entry| self.blockers(&entry.request, ticket.serial).is_empty());
        if ready {
            let request = self
                .remove_pending(ticket.transaction)
                .expect("queued requester");
            self.grant(request);
            Ok(Acquire::Deadlock {
                victims,
                requester: Requester::Granted,
            })
        } else {
            Ok(Acquire::Deadlock {
                victims,
                requester: Requester::Queued(ticket),
            })
        }
    }

    /// Chooses the transaction to abort from a wait cycle. Every member of a
    /// wait cycle is queued, so the manager holds each one's priority. The
    /// lowest priority loses; an exact tie is settled by the fixed transaction
    /// order, with the higher identity (the later transaction) chosen as the
    /// victim.
    fn select_victim(&self, cycle: &[TxId]) -> TxId {
        let mut chosen: Option<(i64, TxId)> = None;
        for entry in self.queue.values() {
            let transaction = entry.ticket.transaction;
            if !cycle.contains(&transaction) {
                continue;
            }
            let candidate = (self.priority(transaction), transaction);
            chosen = Some(match chosen {
                None => candidate,
                Some(current) => {
                    if candidate.0 < current.0
                        || (candidate.0 == current.0 && candidate.1 > current.1)
                    {
                        candidate
                    } else {
                        current
                    }
                }
            });
        }
        chosen.map_or(cycle[0], |(_, transaction)| transaction)
    }

    /// Returns the stored priority for a transaction, defaulting to zero.
    pub fn priority(&self, transaction: TxId) -> i64 {
        self.priorities.get(&transaction).copied().unwrap_or(0)
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
        self.priorities.remove(&transaction);
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

/// Rotates a closed wait path so it starts and ends at `start`, preserving the
/// order of the edges that connect the cycle's members.
fn cycle_from(cycle: &[TxId], start: TxId) -> Vec<TxId> {
    let nodes = &cycle[..cycle.len().saturating_sub(1)];
    let Some(index) = nodes.iter().position(|transaction| *transaction == start) else {
        return cycle.to_vec();
    };
    let mut rotated = Vec::with_capacity(cycle.len());
    rotated.extend_from_slice(&nodes[index..]);
    rotated.extend_from_slice(&nodes[..index]);
    rotated.push(start);
    rotated
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use super::*;
    use crate::rows::Key;

    fn point(key: i64) -> ReadInterval {
        ReadInterval::new(
            "rows",
            Bound::Included(Key::from(key)),
            Bound::Included(Key::from(key)),
        )
    }

    fn enqueue(
        locks: &mut LockManager,
        transaction: TxId,
        key: i64,
        mode: LockMode,
        priority: i64,
    ) -> Acquire {
        let request = LockRequest::new(transaction, point(key), mode);
        locks.enqueue_with_priority(request, priority).unwrap()
    }

    #[test]
    fn cycles_only_count_real_wait_edges() {
        let mut locks = LockManager::default();
        enqueue(&mut locks, 1, 1, LockMode::Exclusive, 0);
        enqueue(&mut locks, 2, 2, LockMode::Exclusive, 0);
        // Two independent waiters that never depend on each other must not
        // register a cycle, so neither is aborted.
        assert!(matches!(
            enqueue(&mut locks, 3, 1, LockMode::Exclusive, 0),
            Acquire::Waiting(_)
        ));
        assert!(matches!(
            enqueue(&mut locks, 4, 2, LockMode::Exclusive, 0),
            Acquire::Waiting(_)
        ));
        assert!(locks.graph().cycle_through(3).is_none());
    }

    #[test]
    fn lowest_priority_member_is_the_victim() {
        let mut locks = LockManager::default();
        enqueue(&mut locks, 1, 1, LockMode::Exclusive, 5);
        enqueue(&mut locks, 2, 2, LockMode::Exclusive, 9);
        enqueue(&mut locks, 1, 2, LockMode::Exclusive, 5);
        let Acquire::Deadlock { victims, requester } =
            enqueue(&mut locks, 2, 1, LockMode::Exclusive, 9)
        else {
            panic!("second transaction closes the cycle");
        };
        assert_eq!(requester, Requester::Granted);
        assert_eq!(victims.len(), 1);
        assert_eq!(victims[0].victim, 1);
        assert_eq!(victims[0].cycle, vec![1, 2, 1]);
        // The victim's priority is forgotten once it is released.
        assert_eq!(locks.priority(1), 0);
        assert_eq!(locks.priority(2), 9);
    }

    #[test]
    fn equal_priority_breaks_ties_by_transaction_identity() {
        let mut locks = LockManager::default();
        enqueue(&mut locks, 7, 1, LockMode::Exclusive, 0);
        enqueue(&mut locks, 8, 2, LockMode::Exclusive, 0);
        enqueue(&mut locks, 7, 2, LockMode::Exclusive, 0);
        let Acquire::Deadlock { victims, requester } =
            enqueue(&mut locks, 8, 1, LockMode::Exclusive, 0)
        else {
            panic!("cycle expected");
        };
        // Transaction 8 is later and ties on priority, so it is the victim even
        // though it is the one that closed the cycle.
        assert_eq!(requester, Requester::Victim);
        assert_eq!(victims[0].victim, 8);
        assert_eq!(victims[0].cycle, vec![8, 7, 8]);
    }

    #[test]
    fn a_victim_that_was_queued_keeps_unrelated_waiters() {
        let mut locks = LockManager::default();
        enqueue(&mut locks, 1, 1, LockMode::Exclusive, 5);
        enqueue(&mut locks, 2, 2, LockMode::Exclusive, 9);
        // Transaction 3 is a bystander queued behind transaction 1 on row 1.
        let Acquire::Waiting(bystander) = enqueue(&mut locks, 3, 1, LockMode::Exclusive, 9) else {
            panic!("bystander must wait");
        };
        enqueue(&mut locks, 1, 2, LockMode::Exclusive, 5);
        let Acquire::Deadlock { victims, .. } = enqueue(&mut locks, 2, 1, LockMode::Exclusive, 9)
        else {
            panic!("cycle expected");
        };
        assert_eq!(victims[0].victim, 1);
        // The bystander survives and now leads the queue for row 1, so it can
        // claim its ticket with its original request.
        assert_eq!(locks.claim(bystander), Ok(Acquire::Granted));
    }

    #[test]
    fn a_low_priority_requester_aborts_itself() {
        let mut locks = LockManager::default();
        enqueue(&mut locks, 1, 1, LockMode::Exclusive, 9);
        enqueue(&mut locks, 2, 2, LockMode::Exclusive, 5);
        enqueue(&mut locks, 1, 2, LockMode::Exclusive, 9);
        let Acquire::Deadlock { victims, requester } =
            enqueue(&mut locks, 2, 1, LockMode::Exclusive, 5)
        else {
            panic!("cycle expected");
        };
        assert_eq!(requester, Requester::Victim);
        assert_eq!(victims[0].victim, 2);
        assert_eq!(victims[0].cycle, vec![2, 1, 2]);
    }

    #[test]
    fn a_three_transaction_cycle_aborts_only_the_lowest() {
        let mut locks = LockManager::default();
        enqueue(&mut locks, 1, 1, LockMode::Exclusive, 3);
        enqueue(&mut locks, 2, 2, LockMode::Exclusive, 7);
        enqueue(&mut locks, 3, 3, LockMode::Exclusive, 5);
        enqueue(&mut locks, 1, 2, LockMode::Exclusive, 3);
        enqueue(&mut locks, 2, 3, LockMode::Exclusive, 7);
        let Acquire::Deadlock { victims, requester } =
            enqueue(&mut locks, 3, 1, LockMode::Exclusive, 5)
        else {
            panic!("cycle expected");
        };
        // The requester (priority 5) is not the lowest, so it survives and is
        // granted once the priority-3 transaction is aborted.
        assert_eq!(requester, Requester::Granted);
        assert_eq!(victims.len(), 1);
        assert_eq!(victims[0].victim, 1);
        // The reported cycle names every member, starting and ending at victim.
        let cycle = &victims[0].cycle;
        assert_eq!(cycle.first(), Some(&1));
        assert_eq!(cycle.last(), Some(&1));
        assert_eq!(cycle.len(), 4);
        assert!(cycle.contains(&2) && cycle.contains(&3));
    }
}
