use crate::errors::OxiaError;
use crate::errors::OxiaError::UnexpectedStatus;
use crate::operations::Operation;
use crate::operations::ToProtobuf;
use crate::operations::{
    CompletableOperation, DeleteOperation, DeleteRangeOperation, GetOperation, PutOperation,
};
use crate::oxia::{ReadRequest, ReadResponse, WriteRequest};
use crate::provider_manager::ProviderManager;
use crate::shard_manager::ShardManager;
use crate::write_stream_manager::WriteStreamManager;
use std::sync::Arc;
use tonic::codegen::tokio_stream::StreamExt;
use tonic::{Response, Streaming};

pub enum Batch {
    Read(ReadBatch),
    Write(WriteBatch),
}

impl Batch {
    pub fn is_empty(&self) -> bool {
        match self {
            Batch::Read(read_batch) => read_batch.is_empty(),
            Batch::Write(write_batch) => write_batch.is_empty(),
        }
    }
    pub fn can_add(&self, operation: &Operation) -> bool {
        match self {
            Batch::Read(read_batch) => read_batch.can_add(operation),
            Batch::Write(write_batch) => write_batch.can_add(operation),
        }
    }

    pub fn add(&mut self, operation: Operation) {
        match self {
            Batch::Read(read_batch) => read_batch.add(operation),
            Batch::Write(write_batch) => write_batch.add(operation),
        }
    }

    pub async fn flush(&mut self) {
        match self {
            Batch::Read(read_batch) => read_batch.flush().await,
            Batch::Write(write_batch) => write_batch.flush().await,
        }
    }
}

pub(crate) struct ReadBatch {
    get_inflight: Vec<GetOperation>,
    shard_id: i64,
    shard_manager: Arc<ShardManager>,
    provider_manager: Arc<ProviderManager>,
    max_requests: u32,
}
impl ReadBatch {
    pub fn new(
        shard_id: i64,
        shard_manager: Arc<ShardManager>,
        provider_manager: Arc<ProviderManager>,
        max_requests: u32,
    ) -> Self {
        ReadBatch {
            get_inflight: Vec::new(),
            shard_id,
            shard_manager,
            provider_manager,
            max_requests,
        }
    }

    fn is_empty(&self) -> bool {
        self.get_inflight.is_empty()
    }

    fn can_add(&self, _: &Operation) -> bool {
        (self.get_inflight.len() as u32) < self.max_requests
    }
    fn add(&mut self, operation: Operation) {
        if let Operation::Get(get) = operation {
            self.get_inflight.push(get);
        }
    }

    async fn flush(&mut self) {
        let option = self.shard_manager.get_leader(self.shard_id);
        if option.is_none() {
            return;
        }
        let node = option.unwrap();
        match self
            .provider_manager
            .get_provider(node.service_address)
            .await
        {
            Ok(mut provider) => {
                let mut request = ReadRequest {
                    shard: Some(self.shard_id),
                    gets: vec![],
                };
                for operation in self.get_inflight.iter() {
                    request.gets.push(operation.to_proto());
                }
                match provider.read(request).await {
                    Ok(response) => {
                        if let Err(err) = self.receive_all(response).await {
                            self.failure_inflight(err)
                        }
                    }
                    Err(err) => self.failure_inflight(UnexpectedStatus(err.to_string())),
                }
            }
            Err(err) => self.failure_inflight(err),
        }
    }

    async fn receive_all(
        &mut self,
        response: Response<Streaming<ReadResponse>>,
    ) -> Result<(), OxiaError> {
        let mut streaming = response.into_inner();
        let mut inflight_iter = self.get_inflight.drain(..);
        while let Some(next_result) = streaming.next().await {
            let read_response = next_result
                .map_err(|err| UnexpectedStatus(err.to_string()))?;
            for get_response in read_response.gets {
                let next_inflight = inflight_iter.next();
                if next_inflight.is_none() {
                    return Err(UnexpectedStatus(
                        "bug! unexpected inflight response".to_string(),
                    ));
                }
                let mut operation = next_inflight.unwrap();
                operation.complete(get_response);
            }
        }
        Ok(())
    }

    fn failure_inflight(&mut self, err: OxiaError) {
        for mut operation in self.get_inflight.drain(..) {
            operation.complete_exception(err.clone())
        }
    }
}

pub(crate) struct WriteBatch {
    shard_id: i64,
    put_inflight: Vec<PutOperation>,
    delete_inflight: Vec<DeleteOperation>,
    delete_range_inflight: Vec<DeleteRangeOperation>,
    write_stream_manager: Arc<WriteStreamManager>,
    max_requests: u32,
}

impl WriteBatch {
    pub fn new(shard_id: i64, write_stream_manager: Arc<WriteStreamManager>, max_requests: u32) -> Self {
        WriteBatch {
            shard_id,
            put_inflight: Vec::new(),
            delete_inflight: Vec::new(),
            delete_range_inflight: Vec::new(),
            write_stream_manager,
            max_requests,
        }
    }

    fn is_empty(&self) -> bool {
        self.put_inflight.is_empty()
            && self.delete_inflight.is_empty()
            && self.delete_range_inflight.is_empty()
    }

    fn total_count(&self) -> u32 {
        (self.put_inflight.len() + self.delete_inflight.len() + self.delete_range_inflight.len())
            as u32
    }

    fn can_add(&self, _: &Operation) -> bool {
        self.total_count() < self.max_requests
    }

    fn add(&mut self, operation: Operation) {
        match operation {
            Operation::Put(put) => {
                self.put_inflight.push(put);
            }
            Operation::Delete(delete) => {
                self.delete_inflight.push(delete);
            }
            Operation::DeleteRange(delete_range) => {
                self.delete_range_inflight.push(delete_range);
            }
            Operation::Get(_) => {}
        }
    }

    async fn flush(&mut self) {
        let mut write_request = WriteRequest {
            shard: Some(self.shard_id),
            puts: vec![],
            deletes: vec![],
            delete_ranges: vec![],
        };
        for operation in self.put_inflight.iter() {
            write_request.puts.push(operation.to_proto());
        }
        for operation in self.delete_inflight.iter() {
            write_request.deletes.push(operation.to_proto());
        }
        for operation in self.delete_range_inflight.iter() {
            write_request.delete_ranges.push(operation.to_proto());
        }
        match self.write_stream_manager.write(write_request).await {
            Ok(response) => {
                let mut put_responses = response.puts.into_iter();
                for mut operation in self.put_inflight.drain(..) {
                    match put_responses.next() {
                        Some(put_response) => operation.complete(put_response),
                        None => operation.complete_exception(UnexpectedStatus(
                            "missing put response from server".to_string(),
                        )),
                    }
                }
                let mut delete_responses = response.deletes.into_iter();
                for mut operation in self.delete_inflight.drain(..) {
                    match delete_responses.next() {
                        Some(delete_response) => operation.complete(delete_response),
                        None => operation.complete_exception(UnexpectedStatus(
                            "missing delete response from server".to_string(),
                        )),
                    }
                }
                let mut delete_range_responses = response.delete_ranges.into_iter();
                for mut operation in self.delete_range_inflight.drain(..) {
                    match delete_range_responses.next() {
                        Some(delete_range_response) => {
                            operation.complete(delete_range_response)
                        }
                        None => operation.complete_exception(UnexpectedStatus(
                            "missing delete_range response from server".to_string(),
                        )),
                    }
                }
            }
            Err(err) => {
                self.failure_inflight(err);
            }
        }
    }

    fn failure_inflight(&mut self, err: OxiaError) {
        for mut operation in self.put_inflight.drain(..) {
            operation.complete_exception(err.clone())
        }
        for mut operation in self.delete_inflight.drain(..) {
            operation.complete_exception(err.clone())
        }
        for mut operation in self.delete_range_inflight.drain(..) {
            operation.complete_exception(err.clone())
        }
    }
}
