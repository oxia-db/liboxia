//! Streaming result types: ordered range scans / lists merged across shards,
//! and the notification / sequence-update subscription handles.

use crate::errors::OxiaError;
use crate::key;
use crate::types::{GetResult, Notification};
use futures::Stream;
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;

/// A boxed stream of results coming from a single shard.
pub(crate) type ShardStream<T> = Pin<Box<dyn Stream<Item = Result<T, OxiaError>> + Send>>;

/// Items that can be merged across shards in key order.
pub(crate) trait MergeItem {
    fn merge_key(&self) -> &str;
}

impl MergeItem for String {
    fn merge_key(&self) -> &str {
        self
    }
}

impl MergeItem for GetResult {
    fn merge_key(&self) -> &str {
        &self.key
    }
}

/// An ordered k-way merge over per-shard streams.
///
/// Each shard returns its results in Oxia's key order; holding at most one
/// pending item per shard and always yielding the smallest keeps the global
/// order identical to the server's while buffering O(shards) items.
pub(crate) struct Merged<T> {
    streams: Vec<ShardStream<T>>,
    peeked: Vec<Option<T>>,
    done: Vec<bool>,
    finished: bool,
}

pub(crate) fn merged<T>(streams: Vec<ShardStream<T>>) -> Merged<T> {
    let count = streams.len();
    Merged {
        streams,
        peeked: (0..count).map(|_| None).collect(),
        done: vec![false; count],
        finished: false,
    }
}

impl<T: MergeItem + Unpin> Stream for Merged<T> {
    type Item = Result<T, OxiaError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.finished {
            return Poll::Ready(None);
        }
        // Fill every empty slot; a stream that is Pending keeps the whole merge
        // Pending (its waker is registered), because yielding without knowing
        // its next key could break the global order.
        let mut pending = false;
        for i in 0..this.streams.len() {
            while this.peeked[i].is_none() && !this.done[i] {
                match this.streams[i].as_mut().poll_next(cx) {
                    Poll::Ready(Some(Ok(item))) => this.peeked[i] = Some(item),
                    Poll::Ready(Some(Err(err))) => {
                        this.finished = true;
                        return Poll::Ready(Some(Err(err)));
                    }
                    Poll::Ready(None) => this.done[i] = true,
                    Poll::Pending => {
                        pending = true;
                        break;
                    }
                }
            }
        }
        if pending {
            return Poll::Pending;
        }
        let mut min_idx: Option<usize> = None;
        for i in 0..this.peeked.len() {
            if let (Some(candidate), Some(m)) = (&this.peeked[i], min_idx) {
                let current = this.peeked[m].as_ref().expect("min slot is filled");
                if key::compare(candidate.merge_key(), current.merge_key()).is_lt() {
                    min_idx = Some(i);
                }
            } else if this.peeked[i].is_some() && min_idx.is_none() {
                min_idx = Some(i);
            }
        }
        match min_idx {
            Some(i) => Poll::Ready(Some(Ok(this.peeked[i].take().expect("slot is filled")))),
            None => {
                this.finished = true;
                Poll::Ready(None)
            }
        }
    }
}

/// An ordered stream of keys produced by
/// [`ListBuilder::stream`](crate::ListBuilder::stream).
///
/// Yields keys in Oxia's slash-aware key order, merged across shards, with
/// bounded memory. Ends after yielding an error.
pub struct ListStream {
    inner: Merged<String>,
}

impl ListStream {
    pub(crate) fn new(inner: Merged<String>) -> Self {
        ListStream { inner }
    }
}

impl Stream for ListStream {
    type Item = Result<String, OxiaError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

impl fmt::Debug for ListStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ListStream").finish_non_exhaustive()
    }
}

/// An ordered stream of records produced by
/// [`RangeScanBuilder::stream`](crate::RangeScanBuilder::stream).
///
/// Yields records in Oxia's slash-aware key order, merged across shards, with
/// bounded memory. Ends after yielding an error.
pub struct RangeScanStream {
    inner: Merged<GetResult>,
}

impl RangeScanStream {
    pub(crate) fn new(inner: Merged<GetResult>) -> Self {
        RangeScanStream { inner }
    }
}

impl Stream for RangeScanStream {
    type Item = Result<GetResult, OxiaError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

impl fmt::Debug for RangeScanStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RangeScanStream").finish_non_exhaustive()
    }
}

/// A subscription to database change notifications, created with
/// [`OxiaClient::notifications`](crate::OxiaClient::notifications).
///
/// Consume it with [`recv`](Notifications::recv) or as a [`Stream`]. Dropping
/// it releases the subscription.
#[derive(Debug)]
pub struct Notifications {
    rx: mpsc::Receiver<Notification>,
}

impl Notifications {
    pub(crate) fn new(rx: mpsc::Receiver<Notification>) -> Self {
        Notifications { rx }
    }

    /// Receives the next notification; `None` once the client is closed.
    pub async fn recv(&mut self) -> Option<Notification> {
        self.rx.recv().await
    }
}

impl Stream for Notifications {
    type Item = Notification;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

/// A subscription to sequence advances, created with
/// [`OxiaClient::sequence_updates`](crate::OxiaClient::sequence_updates).
///
/// Yields the highest assigned sequence key each time the sequence advances.
/// Consume it with [`recv`](SequenceUpdates::recv) or as a [`Stream`].
#[derive(Debug)]
pub struct SequenceUpdates {
    rx: mpsc::Receiver<String>,
}

impl SequenceUpdates {
    pub(crate) fn new(rx: mpsc::Receiver<String>) -> Self {
        SequenceUpdates { rx }
    }

    /// Receives the next sequence key; `None` once the client is closed.
    pub async fn recv(&mut self) -> Option<String> {
        self.rx.recv().await
    }
}

impl Stream for SequenceUpdates {
    type Item = String;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    fn shard(items: Vec<Result<String, OxiaError>>) -> ShardStream<String> {
        Box::pin(futures::stream::iter(items))
    }

    fn keys(items: &[&str]) -> ShardStream<String> {
        shard(items.iter().map(|s| Ok(s.to_string())).collect())
    }

    #[tokio::test]
    async fn merges_in_key_order() {
        let merged = merged(vec![
            keys(&["a", "d", "e"]),
            keys(&["b", "c", "f"]),
            keys(&[]),
        ]);
        let out: Vec<String> = merged.map(|r| r.unwrap()).collect().await;
        assert_eq!(out, vec!["a", "b", "c", "d", "e", "f"]);
    }

    #[tokio::test]
    async fn merges_with_slash_order() {
        // "a" sorts before "/" in Oxia's slash-aware order.
        let merged = merged(vec![keys(&["/x"]), keys(&["a"])]);
        let out: Vec<String> = merged.map(|r| r.unwrap()).collect().await;
        assert_eq!(out, vec!["a", "/x"]);
    }

    #[tokio::test]
    async fn error_terminates_stream() {
        let merged = merged(vec![
            shard(vec![Ok("a".to_string()), Err(OxiaError::Timeout)]),
            keys(&["b", "c"]),
        ]);
        let out: Vec<Result<String, OxiaError>> = merged.collect().await;
        assert!(matches!(out[0], Ok(ref k) if k == "a"));
        assert!(out.iter().any(|r| r.is_err()));
        // Nothing after the error.
        let err_pos = out.iter().position(|r| r.is_err()).unwrap();
        assert_eq!(err_pos, out.len() - 1);
    }

    #[tokio::test]
    async fn empty_merge_ends() {
        let merged = merged::<String>(vec![]);
        let out: Vec<_> = merged.collect().await;
        assert!(out.is_empty());
    }
}
