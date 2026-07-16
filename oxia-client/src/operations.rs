//! Internal plumbing between request builders and the per-shard batchers: a
//! pending operation is the already-built wire request plus the oneshot used to
//! deliver its response. Requests are *moved* into the outgoing batch at flush
//! time — values are `Bytes`, so nothing is deep-copied on the write path.

use crate::errors::OxiaError;
use crate::proto;
use tokio::sync::oneshot;

/// A wire request paired with the channel that completes the caller's future.
pub(crate) struct Pending<Req, Resp> {
    pub(crate) request: Req,
    pub(crate) callback: oneshot::Sender<Result<Resp, OxiaError>>,
}

impl<Req, Resp> Pending<Req, Resp> {
    pub(crate) fn new(request: Req) -> (Self, oneshot::Receiver<Result<Resp, OxiaError>>) {
        let (tx, rx) = oneshot::channel();
        (
            Pending {
                request,
                callback: tx,
            },
            rx,
        )
    }
}

pub(crate) type PendingPut = Pending<proto::PutRequest, proto::PutResponse>;
pub(crate) type PendingDelete = Pending<proto::DeleteRequest, proto::DeleteResponse>;
pub(crate) type PendingDeleteRange = Pending<proto::DeleteRangeRequest, proto::DeleteRangeResponse>;
pub(crate) type PendingGet = Pending<proto::GetRequest, proto::GetResponse>;
