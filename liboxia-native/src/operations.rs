use crate::client::{
    DeleteOption, DeleteRangeOption, GetNotificationOption, GetOption, GetSequenceUpdatesOption,
    ListOption, PutOption, RangeScanOption,
};
use crate::errors::OxiaError;
use crate::oxia::{
    DeleteRangeRequest, DeleteRangeResponse, DeleteRequest, DeleteResponse, GetRequest,
    GetResponse, KeyComparisonType, ListRequest, PutRequest, PutResponse, RangeScanRequest,
    SecondaryIndex,
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

    // internal status for helping to decide whether to use the session
    pub(crate) ephemeral: bool,
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
                }
                PutOption::Ephemeral() => operation.ephemeral = true,
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

#[derive(Default)]
pub(crate) struct DeleteOperation {
    pub(crate) callback: Option<Sender<Result<DeleteResponse, OxiaError>>>,
    pub(crate) key: String,
    pub(crate) expected_version_id: Option<i64>,
    pub(crate) partition_key: Option<String>,
}

impl ToProtobuf<DeleteRequest> for DeleteOperation {
    fn to_proto(&self) -> DeleteRequest {
        DeleteRequest {
            key: self.key.clone(),
            expected_version_id: self.expected_version_id,
        }
    }
}

impl From<Vec<DeleteOption>> for DeleteOperation {
    fn from(options: Vec<DeleteOption>) -> Self {
        let mut operation = DeleteOperation::default();
        for option in options {
            match option {
                DeleteOption::ExpectVersionId(expected_version_id) => {
                    operation.expected_version_id = Some(expected_version_id)
                }
                DeleteOption::PartitionKey(partition_key) => {
                    operation.partition_key = Some(partition_key)
                }
                DeleteOption::RecordDoesNotExist() => operation.expected_version_id = Some(-1),
            }
        }
        operation
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

#[derive(Default)]
pub struct DeleteRangeOperation {
    pub(crate) callback: Option<Sender<Result<DeleteRangeResponse, OxiaError>>>,
    pub(crate) partition_key: Option<String>,
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

impl From<Vec<DeleteRangeOption>> for DeleteRangeOperation {
    fn from(options: Vec<DeleteRangeOption>) -> Self {
        let mut operation = DeleteRangeOperation::default();
        for option in options {
            match option {
                DeleteRangeOption::PartitionKey(partition_key) => {
                    operation.partition_key = Some(partition_key)
                }
            }
        }
        operation
    }
}
impl Clone for DeleteRangeOperation {
    fn clone(&self) -> Self {
        DeleteRangeOperation {
            callback: None,
            partition_key: self.partition_key.clone(),
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

#[derive(Default)]
pub(crate) struct GetOperation {
    pub(crate) callback: Option<Sender<Result<GetResponse, OxiaError>>>,
    pub(crate) partition_key: Option<String>,
    pub(crate) key: String,
    pub(crate) include_value: bool,
    pub(crate) comparison_type: KeyComparisonType,
    pub(crate) secondary_index_name: Option<String>,
}

impl Clone for GetOperation {
    fn clone(&self) -> Self {
        GetOperation {
            callback: None,
            partition_key: self.partition_key.clone(),
            key: self.key.clone(),
            include_value: self.include_value,
            comparison_type: self.comparison_type.clone(),
            secondary_index_name: self.secondary_index_name.clone(),
        }
    }
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

impl From<Vec<GetOption>> for GetOperation {
    fn from(options: Vec<GetOption>) -> Self {
        let mut operation = GetOperation::default();
        for option in options {
            match option {
                GetOption::PartitionKey(partition_key) => {
                    operation.partition_key = Some(partition_key)
                }
                GetOption::ComparisonType(comparison_type) => {
                    operation.comparison_type = comparison_type
                }
                GetOption::IncludeValue() => {
                    operation.include_value = true;
                }
                GetOption::UseIndex(index) => {
                    operation.secondary_index_name = Some(index);
                }
            }
        }
        operation
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

#[derive(Default)]
pub(crate) struct ListOperation {
    pub(crate) shard_id: i64,
    pub(crate) min_key_inclusive: String,
    pub(crate) max_key_exclusive: String,
    pub(crate) partition_key: Option<String>,
    pub(crate) secondary_index_name: Option<String>,
}

impl Clone for ListOperation {
    fn clone(&self) -> Self {
        ListOperation {
            shard_id: self.shard_id,
            min_key_inclusive: self.min_key_inclusive.clone(),
            max_key_exclusive: self.max_key_exclusive.clone(),
            partition_key: self.partition_key.clone(),
            secondary_index_name: self.secondary_index_name.clone(),
        }
    }
}

impl ToProtobuf<ListRequest> for ListOperation {
    fn to_proto(&self) -> ListRequest {
        ListRequest {
            shard: Some(self.shard_id),
            start_inclusive: self.min_key_inclusive.clone(),
            end_exclusive: self.max_key_exclusive.clone(),
            secondary_index_name: self.secondary_index_name.clone(),
        }
    }
}

impl From<Vec<ListOption>> for ListOperation {
    fn from(options: Vec<ListOption>) -> Self {
        let mut operation = ListOperation::default();
        for option in options {
            match option {
                ListOption::PartitionKey(partition_key) => {
                    operation.partition_key = Some(partition_key)
                }
                ListOption::UseIndex(index) => {
                    operation.secondary_index_name = Some(index);
                }
            }
        }
        operation
    }
}

#[derive(Default)]
pub(crate) struct RangeScanOperation {
    pub(crate) shard_id: i64,
    pub(crate) min_key_inclusive: String,
    pub(crate) max_key_exclusive: String,
    pub(crate) partition_key: Option<String>,
    pub(crate) secondary_index_name: Option<String>,
}

impl Clone for RangeScanOperation {
    fn clone(&self) -> Self {
        RangeScanOperation {
            shard_id: self.shard_id,
            min_key_inclusive: self.min_key_inclusive.clone(),
            max_key_exclusive: self.max_key_exclusive.clone(),
            partition_key: self.partition_key.clone(),
            secondary_index_name: self.secondary_index_name.clone(),
        }
    }
}

impl From<Vec<RangeScanOption>> for RangeScanOperation {
    fn from(options: Vec<RangeScanOption>) -> Self {
        let mut operation = RangeScanOperation::default();
        for option in options {
            match option {
                RangeScanOption::PartitionKey(partition_key) => {
                    operation.partition_key = Some(partition_key)
                }
                RangeScanOption::UseIndex(index) => {
                    operation.secondary_index_name = Some(index);
                }
            }
        }
        operation
    }
}

impl ToProtobuf<RangeScanRequest> for RangeScanOperation {
    fn to_proto(&self) -> RangeScanRequest {
        RangeScanRequest {
            shard: Some(self.shard_id),
            start_inclusive: self.min_key_inclusive.clone(),
            end_exclusive: self.max_key_exclusive.clone(),
            secondary_index_name: self.secondary_index_name.clone(),
        }
    }
}

pub(crate) struct GetSequenceUpdatesOperation {
    pub(crate) partition_key: Option<String>,
    pub(crate) buffer_size: usize,
}

impl Default for GetSequenceUpdatesOperation {
    fn default() -> Self {
        GetSequenceUpdatesOperation {
            partition_key: None,
            buffer_size: 100,
        }
    }
}

impl From<Vec<GetSequenceUpdatesOption>> for GetSequenceUpdatesOperation {
    fn from(options: Vec<GetSequenceUpdatesOption>) -> Self {
        let mut operation = GetSequenceUpdatesOperation::default();
        for option in options {
            match option {
                GetSequenceUpdatesOption::PartitionKey(partition_key) => {
                    operation.partition_key = Some(partition_key)
                }
                GetSequenceUpdatesOption::BufferSize(buffer_size) => {
                    operation.buffer_size = buffer_size
                }
            }
        }
        operation
    }
}

pub(crate) struct GetNotificationOperation {
    pub(crate) buffer_size: usize,
}

impl Default for GetNotificationOperation {
    fn default() -> Self {
        GetNotificationOperation { buffer_size: 100 }
    }
}

impl From<Vec<GetNotificationOption>> for GetNotificationOperation {
    fn from(options: Vec<GetNotificationOption>) -> Self {
        let mut operation = GetNotificationOperation::default();
        for option in options {
            match option {
                GetNotificationOption::BufferSize(buffer_size) => {
                    operation.buffer_size = buffer_size
                }
            }
        }
        operation
    }
}

pub enum Operation {
    Put(PutOperation),
    Delete(DeleteOperation),
    DeleteRange(DeleteRangeOperation),
    Get(GetOperation),
}
