use crate::address::ensure_protocol;
use crate::errors::OxiaError;
use crate::errors::OxiaError::UnexpectedStatus;
use crate::oxia::shard_assignment::ShardBoundaries;
use crate::oxia::{ShardAssignment, ShardAssignmentsRequest};
use crate::provider_manager::ProviderManager;
use backoff::ExponentialBackoff;
use dashmap::DashMap;
use log::{info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::codegen::tokio_stream::StreamExt;
use tonic::Status;

pub(crate) struct Node {
    pub service_address: String,
}

pub struct ShardManagerOptions {
    pub address: String,
    pub namespace: String,
    pub provider_manager: Arc<ProviderManager>,
}

pub struct ShardManager {
    namespace: String,
    inner: Arc<Inner>,

    context: CancellationToken,
    assignment_handle: JoinHandle<()>,
}

struct Inner {
    provider_manager: Arc<ProviderManager>,
    current_assignments: Arc<DashMap<i64, ShardAssignment>>,
}

async fn start_assignments_listener(
    context: CancellationToken,
    address: String,
    namespace: String,
    inner: Arc<Inner>,
    init_tx: mpsc::Sender<()>,
) {
    let op_defer = || {
        let assignments_ref = inner.current_assignments.clone();
        let local_address = address.clone();
        let local_inner = inner.clone();
        let local_context = context.clone();
        let ns = namespace.clone();
        let init_sender = init_tx.clone();
        async move {
            let provider = local_inner
                .provider_manager
                .get_provider(local_address.as_str())
                .await
                .map_err(|err| backoff::Error::transient(Status::internal(err.to_string())))?;
            let mut guard_provider = provider.lock().await;
            let mut streaming = guard_provider
                .get_shard_assignments(ShardAssignmentsRequest {
                    namespace: ns.clone(),
                })
                .await?
                .into_inner();
            drop(guard_provider);
            loop {
                tokio::select! {
                    _ = local_context.cancelled() => {
                        info!("Shards assignments listener exit due to cancellation.");
                        return Ok(());
                    }
                    result = streaming.next() => {
                        if result.is_none() {
                                continue;
                        }
                        match result.unwrap() {
                        Ok(assignments) => {
                            let ns_assignments = assignments.namespaces.get(&ns);
                            if ns_assignments.is_none() {
                                    continue;
                            }
                            assignments_ref.clear();
                            for sa in &ns_assignments.unwrap().assignments {
                                assignments_ref.insert(sa.shard, sa.clone());
                            }
                            if !init_sender.is_closed() {
                                let _ = init_sender.send(()).await;
                            }
                        }
                        Err(stream_status) => {
                             return Err(backoff::Error::transient(stream_status))
                        }
                    }
                    }
                }
            }
        }
    };
    let backoff = ExponentialBackoff::default();
    let _ = backoff::future::retry_notify(backoff, op_defer, |err, duration| {
        warn!(
            "Transient failure receiving shard assignments. error: {:?} retry-after: {:?}.",
            err, duration
        )
    })
    .await;
}

impl ShardManager {
    pub async fn new(options: ShardManagerOptions) -> Result<Self, OxiaError> {
        let context = CancellationToken::new();
        let inner = Arc::new(Inner {
            provider_manager: options.provider_manager,
            current_assignments: Arc::new(DashMap::new()),
        });
        let (init_tx, mut init_rx) = mpsc::channel(1);
        let assignment_handle = tokio::spawn(start_assignments_listener(
            context.clone(),
            options.address,
            options.namespace.clone(),
            inner.clone(),
            init_tx,
        ));

        init_rx.recv().await;
        let sm = ShardManager {
            namespace: options.namespace,
            inner,
            context,
            assignment_handle,
        };
        Ok(sm)
    }

    pub async fn shutdown(self) -> Result<(), OxiaError> {
        self.context.cancel();
        self.assignment_handle
            .await
            .map_err(|err| UnexpectedStatus(err.to_string()))?;
        Ok(())
    }

    pub fn get_leader(&self, shard_id: i64) -> Option<Node> {
        let assignment_option = self.inner.current_assignments.get(&shard_id);
        assignment_option.map(|v| Node {
            service_address: ensure_protocol(v.leader.clone()),
        })
    }

    pub fn get_shard(&self, key: &str) -> Option<i64> {
        let code = xxhash_rust::xxh32::xxh32(key.as_bytes(), 0);
        for entry in self.inner.current_assignments.iter() {
            match entry.shard_boundaries.unwrap() {
                ShardBoundaries::Int32HashRange(range) => {
                    if range.min_hash_inclusive <= code && code <= range.max_hash_inclusive {
                        return Some(entry.shard);
                    }
                }
            }
        }
        None
    }

    // todo: consider iterator to avoid clone
    pub fn get_shards_leader(&self) -> HashMap<i64, Node> {
        let mut map = HashMap::with_capacity(self.inner.current_assignments.len());
        for item in self.inner.current_assignments.iter() {
            map.insert(
                item.shard,
                Node {
                    service_address: ensure_protocol(item.leader.clone()),
                },
            );
        }
        map
    }

    pub fn get_shard_leader(&self, key: &str) -> Option<Node> {
        let code = xxhash_rust::xxh32::xxh32(key.as_bytes(), 0);
        for entry in self.inner.current_assignments.iter() {
            match entry.shard_boundaries.unwrap() {
                ShardBoundaries::Int32HashRange(range) => {
                    if range.min_hash_inclusive <= code && code <= range.max_hash_inclusive {
                        return Some(Node {
                            service_address: ensure_protocol(entry.leader.clone()),
                        });
                    }
                }
            }
        }
        None
    }
}
