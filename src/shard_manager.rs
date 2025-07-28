use crate::errors::OxiaError;
use crate::oxia::oxia_client_client::OxiaClientClient;
use crate::oxia::shard_assignment::ShardBoundaries;
use crate::oxia::{ShardAssignment, ShardAssignmentsRequest};
use backon::{ExponentialBuilder, Retryable};
use dashmap::DashMap;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;
use tonic::codegen::tokio_stream::StreamExt;
use tonic::transport::Channel;
use tonic::Status;

pub(crate) struct Node {
    pub service_address: String,
}

pub trait ShardManager {
    fn get_leader(&self, shard_id: i64) -> Option<Node>;
    fn get_shard_leader(&self, key: &str) -> Option<Node>;
}

pub struct ShardManagerOptions {
    pub namespace: String,
    pub runtime: Arc<Runtime>,
    pub provider: OxiaClientClient<Channel>,
}

pub struct ShardManagerImpl {
    namespace: String,
    runtime: Arc<Runtime>,
    provider: OxiaClientClient<Channel>,

    current_assignments: Arc<DashMap<i64, ShardAssignment>>,
}

impl ShardManagerImpl {
    pub fn new(options: ShardManagerOptions) -> Result<Self, OxiaError> {
        let sm = ShardManagerImpl {
            namespace: options.namespace,
            runtime: options.runtime,
            provider: options.provider,
            current_assignments: Arc::new(DashMap::new()),
        };
        let (tx, rx) = oneshot::channel();
        sm.start_assignments_listener(tx);
        let status = sm.runtime.block_on(rx).unwrap();
        if status.code() == tonic::Code::Ok {
            Ok(sm)
        } else {
            Err(OxiaError::Connection(status.message().to_string()))
        }
    }

    pub fn start_assignments_listener(&self, init_tx: Sender<Status>) {
        let shard_runtime = self.runtime.clone();
        let namespace = self.namespace.clone();
        let mut provider = self.provider.clone();

        let stream_result =
            shard_runtime.block_on(provider.get_shard_assignments(ShardAssignmentsRequest {
                namespace: namespace.clone(),
            }));
        if stream_result.is_err() {
            init_tx.send(stream_result.unwrap_err()).unwrap();
            return;
        }
        let mut streaming = stream_result.unwrap().into_inner();
        let mut init_tx_op = Some(init_tx);
        let assignments_ref = self.current_assignments.clone();

        self.runtime.spawn(async move {
            let mut initialized = false;
            loop {
                let option = streaming.next().await;
                if option.is_none() {
                    continue;
                }
                match option.unwrap() {
                    Ok(assignments) => {
                        let ns_assignments = assignments.namespaces.get(&namespace);
                        if ns_assignments.is_none() {
                            continue;
                        }
                        assignments_ref.clear();
                        for sa in &ns_assignments.unwrap().assignments {
                            assignments_ref.insert(sa.shard, sa.clone());
                        }
                        if !initialized {
                            initialized = true;
                            if let Some(init_tx) = init_tx_op.take() {
                                init_tx.send(Status::ok("initialized")).unwrap();
                            }
                        }
                    }
                    Err(_stream_status) => {
                        streaming = shard_runtime
                            .block_on(
                                (|| async {
                                    provider
                                        .clone()
                                        .get_shard_assignments(ShardAssignmentsRequest {
                                            namespace: namespace.clone(),
                                        })
                                        .await
                                })
                                .retry(ExponentialBuilder::default())
                                .when(|v| v.code() != tonic::Code::Ok),
                            )
                            .unwrap()
                            .into_inner();
                    }
                }
            }
        });
        ()
    }
}

impl ShardManager for ShardManagerImpl {
    fn get_leader(&self, shard_id: i64) -> Option<Node> {
        let assignment_option = self.current_assignments.get(&shard_id);
        assignment_option.map(|v| Node {
            service_address: v.leader.clone(),
        })
    }

    fn get_shard_leader(&self, key: &str) -> Option<Node> {
        let code = xxhash_rust::xxh32::xxh32(key.as_bytes(), 0);
        for entry in self.current_assignments.iter() {
            match entry.shard_boundaries.unwrap() {
                ShardBoundaries::Int32HashRange(range) => {
                    if range.min_hash_inclusive <= code && code <= range.max_hash_inclusive {
                        return Some(Node {
                            service_address: entry.leader.clone(),
                        });
                    }
                }
            }
        }
        None
    }
}
