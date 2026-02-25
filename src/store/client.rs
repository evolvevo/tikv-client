// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use derive_new::new;
use tonic::transport::Channel;

use super::Request;
use crate::proto::tikvpb::tikv_client::TikvClient;
use crate::Config;
use crate::Result;
use crate::SecurityManager;

/// A trait for connecting to TiKV stores.
#[async_trait]
pub trait KvConnect: Sized + Send + Sync + 'static {
    type KvClient: KvClient + Clone + Send + Sync + 'static;

    async fn connect(&self, address: &str) -> Result<Self::KvClient>;
}

#[derive(new, Clone)]
pub struct TikvConnect {
    security_mgr: Arc<SecurityManager>,
    config: Config,
}

#[async_trait]
impl KvConnect for TikvConnect {
    type KvClient = KvRpcClient;

    async fn connect(&self, address: &str) -> Result<KvRpcClient> {
        self.security_mgr
            .connect(address, |ch| {
                // Apply gRPC message size limits from config
                // 0 means unlimited (use usize::MAX)
                let decoding_limit = if self.config.grpc_max_decoding_message_size == 0 {
                    usize::MAX
                } else {
                    self.config.grpc_max_decoding_message_size
                };
                let encoding_limit = if self.config.grpc_max_encoding_message_size == 0 {
                    usize::MAX
                } else {
                    self.config.grpc_max_encoding_message_size
                };

                TikvClient::new(ch)
                    .max_decoding_message_size(decoding_limit)
                    .max_encoding_message_size(encoding_limit)
            })
            .await
            .map(|c| KvRpcClient::new(c, self.config.timeout))
    }
}

#[async_trait]
pub trait KvClient {
    async fn dispatch(&self, req: &dyn Request) -> Result<Box<dyn Any>>;
}

/// This client handles requests for a single TiKV node. It converts the data
/// types and abstractions of the client program into the grpc data types.
#[derive(new, Clone)]
pub struct KvRpcClient {
    rpc_client: TikvClient<Channel>,
    timeout: Duration,
}

#[async_trait]
impl KvClient for KvRpcClient {
    async fn dispatch(&self, request: &dyn Request) -> Result<Box<dyn Any>> {
        request.dispatch(&self.rpc_client, self.timeout).await
    }
}
