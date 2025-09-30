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
use sync::oneshot;
use tokio::sync;
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

struct Inflight<T> {
    future: oneshot::Sender<T>,
    operation: Operation,
}

pub(crate) struct ReadBatch {
    get_inflight: Vec<GetOperation>,
    shard_id: i64,
    shard_manager: Arc<ShardManager>,
    provider_manager: Arc<ProviderManager>,
}
impl ReadBatch {
    pub fn new(
        shard_id: i64,
        shard_manager: Arc<ShardManager>,
        provider_manager: Arc<ProviderManager>,
    ) -> Self {
        ReadBatch {
            get_inflight: Vec::new(),
            shard_id,
            shard_manager,
            provider_manager,
        }
    }

    fn is_empty(&self) -> bool {
        self.get_inflight.is_empty()
    }

    fn can_add(&self, operation: &Operation) -> bool {
        true
    }
    fn add(&mut self, operation: Operation) {
        match operation {
            Operation::Get(get) => {
                self.get_inflight.push(get);
            }
            _ => {}
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
            Ok(provider) => {
                let mut provider_guard = provider.lock().await;
                let mut request = ReadRequest {
                    shard: Some(self.shard_id),
                    gets: vec![],
                };
                for operation in self.get_inflight.iter() {
                    request.gets.push(operation.to_proto());
                }
                match provider_guard.read(request).await {
                    Ok(response) => {
                        let result = self.receive_all(response).await;
                        if result.is_err() {
                            self.failure_inflight(result.unwrap_err())
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
        loop {
            let next = streaming.next().await;
            if next.is_none() {
                // stream is closed
                break;
            }
            let next_result = next.unwrap();
            if next_result.is_err() {
                return Err(UnexpectedStatus(next_result.unwrap_err().to_string()));
            }
            let read_response = next_result?;
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
}

impl WriteBatch {
    pub fn new(shard_id: i64, write_stream_manager: Arc<WriteStreamManager>) -> Self {
        WriteBatch {
            shard_id,
            put_inflight: Vec::new(),
            delete_inflight: Vec::new(),
            delete_range_inflight: Vec::new(),
            write_stream_manager,
        }
    }

    fn is_empty(&self) -> bool {
        self.put_inflight.is_empty()
            && self.delete_inflight.is_empty()
            && self.delete_range_inflight.is_empty()
    }

    fn can_add(&self, operation: &Operation) -> bool {
        true
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
            Ok(mut response) => {
                if let Some(put_response) = response.puts.pop() {
                    if let Some(mut put_operation) = self.put_inflight.pop() {
                        put_operation.complete(put_response);
                    }
                }
                if let Some(delete_response) = response.deletes.pop() {
                    if let Some(mut delete_operation) = self.delete_inflight.pop() {
                        delete_operation.complete(delete_response);
                    }
                }
                if let Some(delete_ranges_response) = response.delete_ranges.pop() {
                    if let Some(mut delete_ranges_operation) = self.delete_range_inflight.pop() {
                        delete_ranges_operation.complete(delete_ranges_response)
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
