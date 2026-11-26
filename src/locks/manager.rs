use std::collections::{BTreeMap, BTreeSet, VecDeque};

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
        if let Some(cycle) = self.cycle_reachable_from(ticket.transaction) {
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

    /// Finds a wait-for cycle closed by `start`, exploring only the portion
    /// of the wait graph reachable from it.
    ///
    /// A cycle involving a freshly queued transaction can only leave that
    /// transaction through its own wait edges, so every cycle through it lies
    /// inside its reachable component. This search therefore resolves the
    /// outgoing edges of just the queued transactions it actually reaches,
    /// instead of materializing the graph for every waiting group. The
    /// frontier order, parent bookkeeping and path reconstruction mirror
    /// [`WaitGraph::cycle_through`] exactly, so the returned cycle (including
    /// which branch wins when several close at once) is the same one a full
    /// graph scan would produce.
    fn cycle_reachable_from(&self, start: TxId) -> Option<Vec<TxId>> {
        // Outgoing edges exist only for transactions still in the wait queue;
        // a granted holder blocks others but waits on nobody.
        let outgoing = |transaction: TxId| -> BTreeSet<TxId> {
            let Some(entry) = self
                .pending
                .get(&transaction)
                .and_then(|serial| self.queue.get(serial))
            else {
                return BTreeSet::new();
            };
            self.blockers(&entry.request, entry.ticket.serial)
                .into_iter()
                .map(|(blocking, _)| blocking)
                .collect()
        };

        let mut discovered_from: BTreeMap<TxId, TxId> = BTreeMap::new();
        let mut frontier = VecDeque::from([start]);
        while let Some(current) = frontier.pop_front() {
            for neighbor in outgoing(current) {
                if neighbor == start {
                    let mut path = vec![current];
                    let mut previous = current;
                    while previous != start {
                        previous = discovered_from[&previous];
                        path.push(previous);
                    }
                    path.reverse();
                    path.push(start);
                    return Some(path);
                }
                if let std::collections::btree_map::Entry::Vacant(entry) =
                    discovered_from.entry(neighbor)
                {
                    entry.insert(current);
                    frontier.push_back(neighbor);
                }
            }
        }
        None
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rows::RowId;

    fn exclusive(transaction: TxId, table: &str, key: i64) -> LockRequest {
        LockRequest::new(
            transaction,
            ReadInterval::point(&RowId::new(table, key)),
            LockMode::Exclusive,
        )
    }

    fn shared(transaction: TxId, table: &str, key: i64) -> LockRequest {
        LockRequest::new(
            transaction,
            ReadInterval::point(&RowId::new(table, key)),
            LockMode::Shared,
        )
    }

    fn ticket(serial: u64, transaction: TxId) -> WaitTicket {
        WaitTicket {
            transaction,
            serial,
        }
    }

    // The localized search and the retained full-graph reference must always
    // agree on whether a cycle exists and on the exact cycle path returned.
    #[test]
    fn local_search_matches_full_graph_reference() {
        let mut locks = LockManager::default();

        // Two independent wait chains without any cycle.
        assert_eq!(
            locks.enqueue(exclusive(1, "t1", 1)).unwrap(),
            Acquire::Granted
        );
        assert!(matches!(
            locks.enqueue(exclusive(2, "t1", 1)).unwrap(),
            Acquire::Waiting(_)
        ));
        assert!(matches!(
            locks.enqueue(exclusive(3, "t1", 1)).unwrap(),
            Acquire::Waiting(_)
        ));
        assert_eq!(
            locks.enqueue(exclusive(4, "t2", 1)).unwrap(),
            Acquire::Granted
        );
        assert!(matches!(
            locks.enqueue(exclusive(5, "t2", 1)).unwrap(),
            Acquire::Waiting(_)
        ));

        // Both detectors agree for every queued transaction across both chains.
        for &serial in locks.queue.keys().collect::<Vec<_>>() {
            let tx = locks.queue[&serial].ticket.transaction;
            assert_eq!(
                locks.cycle_reachable_from(tx),
                locks.graph().cycle_through(tx)
            );
        }

        // Removing the head of the first chain leaves the rest cycle-free and
        // keeps the two detectors in agreement after the mutation.
        let head = locks
            .queue
            .values()
            .find(|entry| entry.ticket.transaction == 2)
            .unwrap()
            .ticket;
        locks.cancel(head).unwrap();
        for &serial in locks.queue.keys().collect::<Vec<_>>() {
            let tx = locks.queue[&serial].ticket.transaction;
            assert_eq!(
                locks.cycle_reachable_from(tx),
                locks.graph().cycle_through(tx),
                "divergence for {tx}"
            );
        }
    }

    // Builds the six-lock three-node ring without triggering enqueue's
    // automatic victim removal, so the localized detector and the full-graph
    // reference can be compared while the cycle is actually present.
    #[test]
    fn detectors_agree_while_a_three_node_ring_is_present() {
        let mut locks = LockManager::default();
        assert_eq!(
            locks.enqueue(exclusive(1, "a", 1)).unwrap(),
            Acquire::Granted
        );
        assert_eq!(
            locks.enqueue(exclusive(2, "b", 1)).unwrap(),
            Acquire::Granted
        );
        assert_eq!(
            locks.enqueue(exclusive(3, "c", 1)).unwrap(),
            Acquire::Granted
        );
        assert!(matches!(
            locks.enqueue(exclusive(2, "a", 1)).unwrap(),
            Acquire::Waiting(_)
        ));
        assert!(matches!(
            locks.enqueue(exclusive(3, "b", 1)).unwrap(),
            Acquire::Waiting(_)
        ));

        // Manually park 1 on c to close 1 -> 3 -> 2 -> 1, bypassing the
        // enqueue-time abort so the ring stays in the queue for comparison.
        let serial = locks.next_serial + 1;
        locks.next_serial = serial;
        let request = exclusive(1, "c", 1);
        locks.pending.insert(1, serial);
        locks.queue.insert(
            serial,
            QueueEntry {
                ticket: ticket(serial, 1),
                request,
            },
        );

        // Every queued transaction must produce an identical verdict and path.
        for &queued_serial in locks.queue.keys().collect::<Vec<_>>() {
            let tx = locks.queue[&queued_serial].ticket.transaction;
            assert_eq!(
                locks.cycle_reachable_from(tx),
                locks.graph().cycle_through(tx),
                "detectors diverge for {tx}"
            );
        }
        // The ring is reachable from every node and returns the BFS path.
        assert_eq!(locks.cycle_reachable_from(1), Some(vec![1, 3, 2, 1]));
        assert_eq!(locks.cycle_reachable_from(2), Some(vec![2, 1, 3, 2]));
        assert_eq!(locks.cycle_reachable_from(3), Some(vec![3, 2, 1, 3]));
    }

    // A real three-node ring returns the same start-to-start path as the full
    // graph, while a side branch off the ring is never named as a victim.
    #[test]
    fn three_node_cycle_path_matches_reference_and_spares_side_branch() {
        let mut locks = LockManager::default();

        // A transaction can hold one granted lock while carrying at most one
        // queued request, so the three-node ring uses six distinct point
        // locks: each node holds its own lock (a, b, c) and waits on the next
        // node's lock (d, e, f).
        //
        //   1 holds a ; 2 waits a   (2 -> 1)
        //   2 holds b ; 3 waits b   (3 -> 2)
        //   3 holds c ; 1 waits c   (1 -> 3)  closes 1 -> 3 -> 2 -> 1
        assert_eq!(
            locks.enqueue(exclusive(1, "a", 1)).unwrap(),
            Acquire::Granted
        );
        assert_eq!(
            locks.enqueue(exclusive(2, "b", 1)).unwrap(),
            Acquire::Granted
        );
        assert_eq!(
            locks.enqueue(exclusive(3, "c", 1)).unwrap(),
            Acquire::Granted
        );

        let wait_2 = match locks.enqueue(exclusive(2, "a", 1)).unwrap() {
            Acquire::Waiting(value) => value,
            other => panic!("expected wait, got {other:?}"),
        };
        let wait_3 = match locks.enqueue(exclusive(3, "b", 1)).unwrap() {
            Acquire::Waiting(value) => value,
            other => panic!("expected wait, got {other:?}"),
        };

        // Side branch: 4 queues behind 3 on b, but is not part of any ring.
        let wait_4 = match locks.enqueue(exclusive(4, "b", 1)).unwrap() {
            Acquire::Waiting(value) => value,
            other => panic!("expected wait, got {other:?}"),
        };
        assert_eq!(locks.cycle_reachable_from(4), None);
        assert_eq!(locks.graph().cycle_through(4), None);

        // Before closing the ring, the full-graph reference and the localized
        // search agree for every queued transaction (all still cycle-free).
        for &serial in locks.queue.keys().collect::<Vec<_>>() {
            let tx = locks.queue[&serial].ticket.transaction;
            assert_eq!(
                locks.cycle_reachable_from(tx),
                locks.graph().cycle_through(tx)
            );
        }

        // 1 joining c closes the ring and must abort 1 with the exact BFS
        // path; the side branch (4) must not appear in the cycle.
        let result = locks.enqueue(exclusive(1, "c", 1)).unwrap();
        match result {
            Acquire::Deadlock(cycle) => {
                assert_eq!(cycle, vec![1, 3, 2, 1]);
                assert!(!cycle.contains(&4));
            }
            other => panic!("expected deadlock, got {other:?}"),
        }
        // The rejected request left no residue; prior waiters remain parked.
        assert!(locks.pending.contains_key(&2));
        assert!(locks.pending.contains_key(&3));
        assert!(locks.pending.contains_key(&4));
        // With the ring-closing edge removed, the reference graph is acyclic.
        assert_eq!(locks.graph().cycle_through(2), None);
        let _ = (wait_2, wait_3, wait_4);
    }

    // Re-detection after release and cancel preserves wait relations and the
    // FIFO readiness verdict, and still agrees with the full-graph reference.
    #[test]
    fn release_and_cancel_keep_verdict_consistent_with_reference() {
        let mut locks = LockManager::default();
        assert_eq!(
            locks.enqueue(exclusive(1, "r", 1)).unwrap(),
            Acquire::Granted
        );
        let second = match locks.enqueue(exclusive(2, "r", 1)).unwrap() {
            Acquire::Waiting(value) => value,
            other => panic!("expected wait, got {other:?}"),
        };
        let third = match locks.enqueue(exclusive(3, "r", 1)).unwrap() {
            Acquire::Waiting(value) => value,
            other => panic!("expected wait, got {other:?}"),
        };

        // Cancelling the head waiter does not let the tail overtake it: 3
        // still blocks on 2's serial via the earlier-waiter rule.
        locks.cancel(second).unwrap();
        assert_eq!(
            locks.claim(third).unwrap(),
            Acquire::Waiting(third),
            "tail must not overtake an earlier conflicting waiter"
        );
        assert_eq!(
            locks.cycle_reachable_from(3),
            locks.graph().cycle_through(3)
        );
        assert_eq!(
            locks.ready(),
            Vec::new(),
            "no waiter is ready while the grant is still held"
        );

        // Releasing the holder makes the surviving waiter ready, in ticket
        // order, with no phantom cycle and matching reference verdict.
        locks.release(1);
        assert_eq!(locks.ready(), vec![third]);
        assert_eq!(locks.cycle_reachable_from(3), None);
        assert_eq!(locks.graph().cycle_through(3), None);
        assert_eq!(locks.claim(third).unwrap(), Acquire::Granted);
    }

    // With hundreds of disjoint waiting groups, enqueueing into one group
    // only inspects that group's reachable slice: none of the unrelated
    // groups can be reported as a cycle, and a ring inside one group is still
    // detected exactly as the full-graph oracle would detect it.
    #[test]
    fn five_hundred_disjoint_groups_are_not_scanned_as_victims() {
        let mut locks = LockManager::default();
        let group_count = 500u64;

        // Each group g uses its own table and forms holder 1000+g with two
        // queued waiters, all disjoint from one another.
        for group in 0..group_count {
            let table = format!("g{group}");
            let holder = 1_000 + group;
            let first = 2_000 + group;
            let second = 3_000 + group;
            assert_eq!(
                locks.enqueue(exclusive(holder, &table, 1)).unwrap(),
                Acquire::Granted
            );
            assert!(matches!(
                locks.enqueue(shared(first, &table, 1)).unwrap(),
                Acquire::Waiting(_)
            ));
            assert!(matches!(
                locks.enqueue(shared(second, &table, 1)).unwrap(),
                Acquire::Waiting(_)
            ));
        }
        assert_eq!(locks.waiting_count(), (group_count * 2) as usize);

        // A brand-new three-node group on separate locks closes a real ring.
        // Each node holds one lock and waits on the next node's lock:
        //   10 holds r_a ; 11 waits r_a ; 11 holds r_b ;
        //   12 waits r_b ; 12 holds r_c ; 10 waits r_c (closes the ring).
        assert_eq!(
            locks.enqueue(exclusive(10, "r_a", 1)).unwrap(),
            Acquire::Granted
        );
        assert_eq!(
            locks.enqueue(exclusive(11, "r_b", 1)).unwrap(),
            Acquire::Granted
        );
        assert_eq!(
            locks.enqueue(exclusive(12, "r_c", 1)).unwrap(),
            Acquire::Granted
        );
        assert!(matches!(
            locks.enqueue(exclusive(11, "r_a", 1)).unwrap(),
            Acquire::Waiting(_)
        ));
        assert!(matches!(
            locks.enqueue(exclusive(12, "r_b", 1)).unwrap(),
            Acquire::Waiting(_)
        ));

        // None of the 500 unrelated groups participates in the verdict.
        let result = locks.enqueue(exclusive(10, "r_c", 1)).unwrap();
        let Acquire::Deadlock(cycle) = result else {
            panic!("expected deadlock in the three-node group, got {result:?}");
        };
        assert_eq!(cycle, vec![10, 12, 11, 10]);
        assert!(cycle.iter().all(|tx| ![1_000u64, 2_000, 3_000]
            .iter()
            .any(|base| (base..&(base + group_count)).contains(&tx))));

        // All disjoint groups remain parked and cycle-free.
        assert_eq!(
            locks.waiting_count(),
            (group_count * 2 + 2) as usize,
            "only the rejected ring request is removed"
        );
        for group in 0..group_count {
            for base in [2_000u64, 3_000u64] {
                let tx = base + group;
                assert_eq!(
                    locks.cycle_reachable_from(tx),
                    None,
                    "disjoint group {group} must never be a victim"
                );
            }
        }
    }

    #[test]
    fn shared_waiters_share_a_grant_without_cycle() {
        let mut locks = LockManager::default();
        assert_eq!(
            locks.enqueue(exclusive(1, "s", 1)).unwrap(),
            Acquire::Granted
        );
        locks.release(1);
        assert_eq!(locks.enqueue(shared(2, "s", 1)).unwrap(), Acquire::Granted);
        assert_eq!(locks.enqueue(shared(3, "s", 1)).unwrap(), Acquire::Granted);
        // An exclusive request waits behind both shared holders, no ring.
        assert!(matches!(
            locks.enqueue(exclusive(4, "s", 1)).unwrap(),
            Acquire::Waiting(_)
        ));
        assert_eq!(locks.cycle_reachable_from(4), None);
        assert_eq!(locks.graph().cycle_through(4), None);
        let serial = locks.pending[&4];
        assert_eq!(locks.ready(), Vec::new());
        assert_eq!(
            locks.claim(ticket(serial, 4)).unwrap(),
            Acquire::Waiting(ticket(serial, 4))
        );
    }
}
