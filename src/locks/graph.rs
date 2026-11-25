use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::rows::{ReadInterval, TxId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Blocker {
    Granted,
    EarlierWaiter,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitEdge {
    pub waiting: TxId,
    pub blocking: TxId,
    pub interval: ReadInterval,
    pub reason: Blocker,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WaitGraph {
    edges: Vec<WaitEdge>,
}

impl WaitGraph {
    pub fn new(mut edges: Vec<WaitEdge>) -> Self {
        edges.sort_by_key(|edge| (edge.waiting, edge.blocking, edge.reason));
        Self { edges }
    }

    pub fn edges(&self) -> &[WaitEdge] {
        &self.edges
    }

    pub fn blockers(&self, transaction: TxId) -> impl Iterator<Item = &WaitEdge> {
        self.edges
            .iter()
            .filter(move |edge| edge.waiting == transaction)
    }

    pub fn cycle_through(&self, start: TxId) -> Option<Vec<TxId>> {
        let mut neighbors: BTreeMap<TxId, BTreeSet<TxId>> = BTreeMap::new();
        for edge in &self.edges {
            neighbors
                .entry(edge.waiting)
                .or_default()
                .insert(edge.blocking);
        }
        let mut discovered_from = BTreeMap::new();
        let mut frontier = VecDeque::from([start]);
        while let Some(current) = frontier.pop_front() {
            let Some(next) = neighbors.get(&current) else {
                continue;
            };
            for &neighbor in next {
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
}
