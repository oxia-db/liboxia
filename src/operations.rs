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

pub struct DeleteOperation {
    pub(crate) callback: Option<Sender<Result<DeleteResponse, OxiaError>>>,
}

impl ToProtobuf<DeleteRequest> for DeleteOperation {
    fn to_proto(&self) -> DeleteRequest {
        DeleteRequest {
            key: "".to_string(),
            expected_version_id: None,
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
}

impl ToProtobuf<DeleteRangeRequest> for DeleteRangeOperation {
    fn to_proto(&self) -> DeleteRangeRequest {
        DeleteRangeRequest {
            start_inclusive: "".to_string(),
            end_exclusive: "".to_string(),
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

pub struct GetOperation {
    pub(crate) callback: Option<Sender<Result<GetResponse, OxiaError>>>,
    key: String,
    include_value: bool,
    comparison_type: KeyComparisonType,
    secondary_index_name: Option<String>,
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
