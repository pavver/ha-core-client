//! Per-subscription event queue between the runner and the subscription's worker task.
//!
//! Bounded for data events; status events are never dropped. When the callback falls
//! behind, entity subscriptions merge consecutive state changes and other subscriptions
//! drop their oldest events. Either way the callback is told with `SubscriptionStatus::Lagged`.
//! `Filter::StateChanges` also drops state changes that keep the state string.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::sync::Notify;

use crate::events::{Event, SubscriptionStatus};
use crate::subscription::Subscription;

struct State {
    items: VecDeque<Event>,
    /// Data events (not statuses) in `items`.
    data_len: usize,
    /// Events dropped or merged since the last `Lagged` was handed out.
    lagged: u64,
    closed: bool,
}

struct Inner {
    subscription: Subscription,
    state: Mutex<State>,
    notify: Notify,
    capacity: usize,
    filter: Filter,
}

/// What the queue does with `StateChanged` events.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Filter {
    /// Nothing special: the oldest events are dropped when full.
    None,
    /// Consecutive changes of the same entity are merged.
    Merge,
    /// Merged, and changes that keep the state string are dropped.
    StateChanges,
}

impl Inner {
    fn lock(&self) -> MutexGuard<'_, State> {
        // The critical sections never panic, but don't propagate poisoning either way.
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// The runner's end. Dropping it closes the queue; the worker drains what is left.
pub(crate) struct QueueTx(Arc<Inner>);

/// The worker's end.
pub(crate) struct QueueRx {
    inner: Arc<Inner>,
}

/// `capacity` bounds data events; see [`Filter`] for how state changes are handled.
pub(crate) fn channel(subscription: Subscription, capacity: usize, filter: Filter) -> (QueueTx, QueueRx) {
    let inner = Arc::new(Inner {
        subscription,
        state: Mutex::new(State { items: VecDeque::new(), data_len: 0, lagged: 0, closed: false }),
        notify: Notify::new(),
        capacity: capacity.max(1),
        filter,
    });
    (QueueTx(inner.clone()), QueueRx { inner })
}

impl QueueTx {
    pub(crate) fn push(&self, event: Event) {
        let inner = &self.0;
        let mut st = inner.lock();
        let state_changes = inner.filter == Filter::StateChanges;
        if matches!(event, Event::Status { .. }) {
            st.items.push_back(event);
        } else if state_changes && keeps_state(&event) {
            // Not a change of the state; filtered, not lost.
        } else {
            let unmerged =
                if inner.filter == Filter::None { Some(event) } else { merge_into_last(&mut st.items, event) };
            match unmerged {
                Some(event) => push_data(&mut st, event, inner.capacity),
                None => {
                    st.lagged += 1;
                    if state_changes && st.items.back().is_some_and(keeps_state) {
                        // The merged changes cancel out (`A → B`, `B → A`): nothing to deliver.
                        st.items.pop_back();
                        st.data_len -= 1;
                        st.lagged += 1;
                    }
                }
            }
        }
        drop(st);
        inner.notify.notify_one();
    }

    /// Queues a status event of this subscription.
    pub(crate) fn push_status(&self, status: SubscriptionStatus) {
        self.push(Event::Status { subscription: self.0.subscription.clone(), status });
    }

    /// Drops everything queued, for a subscription whose callback must not run.
    pub(crate) fn discard(&self) {
        let mut st = self.0.lock();
        st.items.clear();
        st.data_len = 0;
        st.lagged = 0;
    }
}

impl Drop for QueueTx {
    fn drop(&mut self) {
        self.0.lock().closed = true;
        self.0.notify.notify_one();
    }
}

fn push_data(st: &mut State, event: Event, capacity: usize) {
    if st.data_len >= capacity {
        if let Some(pos) = st.items.iter().position(|e| !matches!(e, Event::Status { .. })) {
            st.items.remove(pos);
            st.data_len -= 1;
            st.lagged += 1;
        }
    }
    st.items.push_back(event);
    st.data_len += 1;
}

/// A state change whose state string stays the same (an attribute-only update).
fn keeps_state(event: &Event) -> bool {
    matches!(event, Event::StateChanged { old_state: Some(old), new_state: Some(new), .. } if old.state == new.state)
}

/// Merges `event` into the last queued event if both are state changes of the same
/// entity: the queued event keeps its `old_state` and takes the new `new_state`.
/// Returns the event back if it was not merged.
fn merge_into_last(items: &mut VecDeque<Event>, event: Event) -> Option<Event> {
    let Event::StateChanged { entity_id, new_state, .. } = &event else { return Some(event) };
    match items.back_mut() {
        Some(Event::StateChanged { entity_id: queued_id, new_state: queued_new, .. }) if queued_id == entity_id => {
            *queued_new = new_state.clone();
            None
        }
        _ => Some(event),
    }
}

impl QueueRx {
    /// The next event, or `None` once the queue is closed and drained. A pending
    /// `Lagged` status is handed out before the events that follow the loss.
    pub(crate) async fn recv(&self) -> Option<Event> {
        loop {
            {
                let mut st = self.inner.lock();
                if st.lagged > 0 {
                    let dropped = std::mem::take(&mut st.lagged);
                    let status = SubscriptionStatus::Lagged { dropped };
                    return Some(Event::Status { subscription: self.inner.subscription.clone(), status });
                }
                if let Some(event) = st.items.pop_front() {
                    if !matches!(event, Event::Status { .. }) {
                        st.data_len -= 1;
                    }
                    return Some(event);
                }
                if st.closed {
                    return None;
                }
            }
            // `notify_one` stores a permit if nobody is waiting, so no wakeup is lost.
            self.inner.notify.notified().await;
        }
    }

    pub(crate) fn sub_id(&self) -> u64 {
        self.inner.subscription.id
    }

    /// Events currently waiting.
    pub(crate) fn len(&self) -> usize {
        self.inner.lock().items.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::State as EntityState;
    use serde_json::json;

    fn change(entity: &str, from: &str, to: &str) -> Event {
        let s = |v: &str| Some(EntityState::from_compressed(entity, &json!({"s": v, "lc": 1.0})).unwrap());
        Event::StateChanged { entity_id: entity.into(), old_state: s(from), new_state: s(to) }
    }

    fn status(status: SubscriptionStatus) -> Event {
        Event::Status { subscription: Subscription { id: 1, client: 0 }, status }
    }

    fn describe(e: &Event) -> String {
        match e {
            Event::StateChanged { entity_id, old_state, new_state } => format!(
                "{entity_id}:{}->{}",
                old_state.as_ref().unwrap().state,
                new_state.as_ref().unwrap().state
            ),
            Event::Status { status, .. } => format!("{status:?}"),
            Event::Generic { event_type, .. } => event_type.clone(),
            other => format!("{other:?}"),
        }
    }

    async fn drain(rx: &QueueRx) -> Vec<String> {
        let mut out = Vec::new();
        while rx.len() > 0 || rx.inner.lock().lagged > 0 {
            out.push(describe(&rx.recv().await.unwrap()));
        }
        out
    }

    fn generic(n: u32) -> Event {
        Event::Generic { event_type: format!("e{n}"), data: json!(null) }
    }

    #[tokio::test]
    async fn merges_consecutive_changes_of_one_entity() {
        let (tx, rx) = channel(Subscription { id: 1, client: 0 }, 10, Filter::Merge);
        tx.push(change("a", "1", "2"));
        tx.push(change("a", "2", "3"));
        tx.push(change("a", "3", "4"));
        tx.push(status(SubscriptionStatus::Interrupted));
        tx.push(change("a", "4", "5"));
        assert_eq!(drain(&rx).await, ["Lagged { dropped: 2 }", "a:1->4", "Interrupted", "a:4->5"]);
    }

    #[tokio::test]
    async fn different_entities_are_not_merged() {
        let (tx, rx) = channel(Subscription { id: 1, client: 0 }, 10, Filter::Merge);
        tx.push(change("a", "1", "2"));
        tx.push(change("b", "1", "2"));
        assert_eq!(drain(&rx).await, ["a:1->2", "b:1->2"]);
    }

    #[tokio::test]
    async fn drops_oldest_data_but_keeps_statuses() {
        let (tx, rx) = channel(Subscription { id: 1, client: 0 }, 2, Filter::None);
        tx.push(generic(1));
        tx.push(status(SubscriptionStatus::Interrupted));
        tx.push(generic(2));
        tx.push(generic(3));
        tx.push(generic(4));
        assert_eq!(drain(&rx).await, ["Lagged { dropped: 2 }", "Interrupted", "e3", "e4"]);
    }

    #[tokio::test]
    async fn state_filter_skips_kept_states_and_cancelled_changes() {
        let (tx, rx) = channel(Subscription { id: 1, client: 0 }, 10, Filter::StateChanges);
        tx.push(change("a", "1", "1"));
        tx.push(change("a", "1", "2"));
        tx.push(change("a", "2", "1"));
        tx.push(change("a", "1", "3"));
        // `1 -> 2 -> 1` cancels out; `1 -> 1` is not a change at all.
        assert_eq!(drain(&rx).await, ["Lagged { dropped: 2 }", "a:1->3"]);
    }

    #[tokio::test]
    async fn closes_after_draining() {
        let (tx, rx) = channel(Subscription { id: 1, client: 0 }, 2, Filter::None);
        tx.push(generic(1));
        drop(tx);
        assert_eq!(describe(&rx.recv().await.unwrap()), "e1");
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn wakes_a_waiting_receiver() {
        let (tx, rx) = channel(Subscription { id: 1, client: 0 }, 2, Filter::None);
        let waiter = tokio::spawn(async move { rx.recv().await.map(|e| describe(&e)) });
        tokio::task::yield_now().await;
        tx.push(generic(7));
        assert_eq!(waiter.await.unwrap().as_deref(), Some("e7"));
    }
}
