//! [`EventStream`]: a subscription consumed as a [`Stream`] instead of with a callback.

use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::Stream;

use crate::error::Result;
use crate::events::{SubEvent, Update};
use crate::queue::QueueRx;
use crate::source::into_update;
use crate::subscription::Subscription;
use crate::HAClient;

/// The updates of a subscription as a [`Stream`], from [`HAClient::stream`].
///
/// Yields data events and status changes in order, like a callback would receive them.
/// The stream ends after the subscription's last status (`Closed`, `Unsubscribed` or
/// `Rejected`). A consumer that falls behind is handled like a slow callback: entity
/// changes are merged, other events dropped, and `Lagged` reports it.
///
/// Dropping the stream unsubscribes (use [`unsubscribe`](Self::unsubscribe) to wait for
/// Home Assistant). It holds an [`HAClient`], so, like a
/// [`SubscriptionGuard`](crate::SubscriptionGuard), it keeps the client running.
/// [`HAClient::stop`] does not wait for streams.
#[must_use = "dropping the stream unsubscribes immediately"]
pub struct EventStream<T> {
    client: HAClient,
    subscription: Option<Subscription>,
    inner: Pin<Box<dyn Stream<Item = SubEvent> + Send>>,
    convert: fn(SubEvent) -> Option<T>,
}

impl<T> EventStream<T> {
    pub(crate) fn new(
        client: HAClient,
        subscription: Subscription,
        rx: QueueRx,
        convert: fn(SubEvent) -> Option<T>,
    ) -> Self {
        let inner = futures_util::stream::unfold(rx, |rx| async move { rx.recv().await.map(|event| (event, rx)) });
        Self { client, subscription: Some(subscription), inner: Box::pin(inner), convert }
    }

    /// The subscription, e.g. for [`HAClient::unsubscribe`].
    pub fn subscription(&self) -> &Subscription {
        self.subscription.as_ref().expect("present until dropped")
    }

    /// Unsubscribes now and waits for Home Assistant to confirm.
    pub async fn unsubscribe(mut self) -> Result<()> {
        let subscription = self.subscription.take().expect("present until dropped");
        self.client.unsubscribe(subscription).await
    }
}

impl<T> Stream for EventStream<T> {
    type Item = Update<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Update<T>>> {
        let this = &mut *self;
        loop {
            match this.inner.as_mut().poll_next(cx) {
                Poll::Ready(Some(event)) => {
                    let sub_id = this.subscription.as_ref().map_or(0, Subscription::id);
                    if let Some(update) = into_update(this.convert, event, sub_id) {
                        return Poll::Ready(Some(update));
                    }
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<T> Drop for EventStream<T> {
    fn drop(&mut self) {
        if let Some(subscription) = self.subscription.take() {
            self.client.unsubscribe_detached(subscription);
        }
    }
}
