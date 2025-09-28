use crate::client::PutOption;
use crate::errors::OxiaError;
use crate::oxia::{
    DeleteRangeRequest, DeleteRangeResponse, DeleteRequest, DeleteResponse, GetRequest,
    GetResponse, KeyComparisonType, PutRequest, PutResponse, SecondaryIndex,
};
use log::warn;
use oneshot::Sender;
use tokio::sync::oneshot;

pub trait CompletableOperation<T> {
    fn complete(&mut self, response: T);

    fn complete_exception(&mut self, err: OxiaError);
}

pub trait ToProtobuf<T> {
    fn to_proto(&self) -> T;
}

#[derive(Default)]
pub(crate) struct PutOperation {
    pub(crate) callback: Option<Sender<Result<PutResponse, OxiaError>>>,
    pub(crate) key: String,
    pub(crate) value: Vec<u8>,
    pub(crate) expected_version_id: Option<i64>,
    pub(crate) session_id: Option<i64>,
    pub(crate) client_identity: Option<String>,
    pub(crate) partition_key: Option<String>,
    pub(crate) sequence_key_delta: Vec<u64>,
    pub(crate) secondary_indexes: Vec<SecondaryIndex>,
}

impl From<Vec<PutOption>> for PutOperation {
    fn from(options: Vec<PutOption>) -> Self {
        let mut operation = PutOperation::default();
        for option in options {
            match option {
                PutOption::ExpectVersionId(expected_version_id) => {
                    operation.expected_version_id = Some(expected_version_id)
                }
                PutOption::PartitionKey(partition_key) => {
                    operation.partition_key = Some(partition_key)
                }
                PutOption::SequenceKeyDelta(sequence_key_delta) => {
                    operation.sequence_key_delta = sequence_key_delta
                }
                PutOption::SecondaryIndexes(secondary_indexes) => {
                    operation.secondary_indexes = secondary_indexes
                },
                PutOption::Ephemeral() => {
                    // todo: support session manager
                }
            }
        }
        operation
    }
}

impl ToProtobuf<PutRequest> for PutOperation {
    fn to_proto(&self) -> PutRequest {
        PutRequest {
            key: self.key.clone(),
            value: self.value.clone(),
            expected_version_id: self.expected_version_id,
            session_id: self.session_id,
            client_identity: self.client_identity.clone(),
            partition_key: self.partition_key.clone(),
            sequence_key_delta: self.sequence_key_delta.clone(),
            secondary_indexes: self.secondary_indexes.clone(),
        }
    }
}

impl CompletableOperation<PutResponse> for PutOperation {
    fn complete(&mut self, response: PutResponse) {
        let option = self.callback.take();
        match option {
            Some(sender) => {
                // consider add log
                let _ = sender.send(Ok(response));
            }
            None => {
                warn!("complete callback twice, ignoring")
            }
        }
    }

    fn complete_exception(&mut self, err: OxiaError) {
        let option = self.callback.take();
        match option {
            Some(sender) => {
                // consider add log
                let _ = sender.send(Err(err));
            }
            None => {
                warn!("complete callback twice, ignoring")
            }
        }
    }
}

pub(crate) struct DeleteOperation {
    pub(crate) callback: Option<Sender<Result<DeleteResponse, OxiaError>>>,
    pub(crate) key: String,
    pub(crate) expected_version_id: Option<i64>,
}

impl ToProtobuf<DeleteRequest> for DeleteOperation {
    fn to_proto(&self) -> DeleteRequest {
        DeleteRequest {
            key: self.key.clone(),
            expected_version_id: self.expected_version_id,
        }
    }
}

impl CompletableOperation<DeleteResponse> for DeleteOperation {
    fn complete(&mut self, response: DeleteResponse) {
        let option = self.callback.take();
        match option {
            Some(sender) => {
                // consider add log
                let _ = sender.send(Ok(response));
            }
            None => {
                warn!("complete callback twice, ignoring")
            }
        }
    }

    fn complete_exception(&mut self, err: OxiaError) {
        let option = self.callback.take();
        match option {
            Some(sender) => {
                // consider add log
                let _ = sender.send(Err(err));
            }
            None => {
                warn!("complete callback twice, ignoring")
            }
        }
    }
}

pub struct DeleteRangeOperation {
    pub(crate) callback: Option<Sender<Result<DeleteRangeResponse, OxiaError>>>,
    pub(crate) start_inclusive: String,
    pub(crate) end_exclusive: String,
}

impl ToProtobuf<DeleteRangeRequest> for DeleteRangeOperation {
    fn to_proto(&self) -> DeleteRangeRequest {
        DeleteRangeRequest {
            start_inclusive: self.start_inclusive.clone(),
            end_exclusive: self.end_exclusive.clone(),
        }
    }
}

impl CompletableOperation<DeleteRangeResponse> for DeleteRangeOperation {
    fn complete(&mut self, response: DeleteRangeResponse) {
        let option = self.callback.take();
        match option {
            Some(sender) => {
                // consider add log
                let _ = sender.send(Ok(response));
            }
            None => {
                warn!("complete callback twice, ignoring")
            }
        }
    }

    fn complete_exception(&mut self, err: OxiaError) {
        let option = self.callback.take();
        match option {
            Some(sender) => {
                // consider add log
                let _ = sender.send(Err(err));
            }
            None => {
                warn!("complete callback twice, ignoring")
            }
        }
    }
}

pub(crate) struct GetOperation {
    pub(crate) callback: Option<Sender<Result<GetResponse, OxiaError>>>,
    pub(crate) key: String,
    pub(crate) include_value: bool,
    pub(crate) comparison_type: KeyComparisonType,
    pub(crate) secondary_index_name: Option<String>,
}

impl CompletableOperation<GetResponse> for GetOperation {
    fn complete(&mut self, response: GetResponse) {
        let option = self.callback.take();
        match option {
            Some(sender) => {
                // consider add log
                let _ = sender.send(Ok(response));
            }
            None => {
                warn!("complete callback twice, ignoring")
            }
        }
    }

    fn complete_exception(&mut self, err: OxiaError) {
        let option = self.callback.take();
        match option {
            Some(sender) => {
                // consider add log
                let _ = sender.send(Err(err));
            }
            None => {
                warn!("complete callback twice, ignoring")
            }
        }
    }
}

impl ToProtobuf<GetRequest> for GetOperation {
    fn to_proto(&self) -> GetRequest {
        GetRequest {
            key: self.key.clone(),
            include_value: self.include_value,
            comparison_type: self.comparison_type as i32,
            secondary_index_name: self.secondary_index_name.clone(),
        }
    }
}

pub enum Operation {
    Put(PutOperation),
    Delete(DeleteOperation),
    DeleteRange(DeleteRangeOperation),
    Get(GetOperation),
}
