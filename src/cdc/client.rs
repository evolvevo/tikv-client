// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

//! CDC client implementation.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::channel::mpsc;
use futures::prelude::*;
use futures::stream::BoxStream;
use log::{debug, info, warn};
use papaya::HashMap;
use tonic::transport::Channel;

use super::event::CdcEvent;
use crate::BoundRange;
use crate::Key;
use crate::Result;
use crate::SecurityManager;
use crate::config::Config;
use crate::pd::RetryClient;
use crate::pd::RetryClientTrait;
use crate::proto::cdcpb::change_data_client::ChangeDataClient;
use crate::proto::cdcpb::change_data_request::{KvApi, Register, Request};
use crate::proto::cdcpb::{ChangeDataRequest, Header};
use crate::region::RegionWithLeader;

/// CDC client version string.
/// TiKV requires a minimum version of 6.2.0 for CDC compatibility.
/// Must be valid semver format (no 'v' prefix) as TiKV uses semver::Version::parse().
const CDC_VERSION: &str = "8.5.5";

/// Options for CDC subscription.
#[derive(Debug, Clone)]
pub struct CdcOptions {
    /// The checkpoint timestamp to start from.
    /// Events before this timestamp won't be sent.
    pub checkpoint_ts: u64,
    /// The KV API to subscribe to (TiDB, RawKV, or TxnKV).
    pub kv_api: KvApi,
    /// Whether to filter out loop-back events (events caused by CDC itself).
    pub filter_loop: bool,
}

impl Default for CdcOptions {
    fn default() -> Self {
        Self {
            checkpoint_ts: 0,
            // TiDb is the default API for TransactionClient (used by SurrealDB)
            // RawKV requires API v2 and is for RawClient only
            kv_api: KvApi::TiDb,
            filter_loop: false,
        }
    }
}

impl CdcOptions {
    /// Create CDC options for TiDB API (used by SurrealDB's TransactionClient).
    /// This is the most common option for applications using TiKV transactions.
    pub fn tidb() -> Self {
        Self {
            kv_api: KvApi::TiDb,
            ..Default::default()
        }
    }

    /// Create CDC options for RawKV API.
    /// Note: This requires TiKV to be running in API v2 mode.
    pub fn raw_kv() -> Self {
        Self {
            kv_api: KvApi::RawKv,
            ..Default::default()
        }
    }

    /// Set the checkpoint timestamp.
    pub fn with_checkpoint_ts(mut self, ts: u64) -> Self {
        self.checkpoint_ts = ts;
        self
    }

    /// Enable loop filtering.
    pub fn with_filter_loop(mut self, filter: bool) -> Self {
        self.filter_loop = filter;
        self
    }
}

/// A CDC client for subscribing to change events from TiKV.
///
/// This client connects to the TiKV cluster via PD and sets up CDC streams
/// to receive change events for specified key ranges or all data.
pub struct CdcClient {
    pd: Arc<RetryClient>,
    security_mgr: Arc<SecurityManager>,
    cluster_id: u64,
    request_id_counter: AtomicU64,
    /// Connected CDC clients per store address (read-heavy, rarely written)
    cdc_clients: HashMap<String, ChangeDataClient<Channel>>,
}

impl CdcClient {
    /// Create a new CDC client and connect to the TiKV cluster.
    ///
    /// # Arguments
    ///
    /// * `pd_endpoints` - The PD endpoints to connect to.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use tikv_client::CdcClient;
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// let cdc = CdcClient::new(vec!["127.0.0.1:2379"]).await.unwrap();
    /// # });
    /// ```
    pub async fn new<S: Into<String>>(pd_endpoints: Vec<S>) -> Result<Self> {
        Self::new_with_config(pd_endpoints, Config::default()).await
    }

    /// Create a new CDC client with custom configuration.
    pub async fn new_with_config<S: Into<String>>(
        pd_endpoints: Vec<S>,
        config: Config,
    ) -> Result<Self> {
        info!("Creating CDC client");
        let pd_endpoints: Vec<String> = pd_endpoints.into_iter().map(Into::into).collect();

        let security_mgr = Arc::new(
            if let (Some(ca), Some(cert), Some(key)) =
                (&config.ca_path, &config.cert_path, &config.key_path)
            {
                SecurityManager::load(ca, cert, key)?
            } else {
                SecurityManager::default()
            },
        );

        let pd = Arc::new(
            RetryClient::connect(&pd_endpoints, security_mgr.clone(), config.timeout).await?,
        );

        // Get cluster ID
        let cluster_id = pd.get_cluster_id().await;

        Ok(Self {
            pd,
            security_mgr,
            cluster_id,
            request_id_counter: AtomicU64::new(1),
            cdc_clients: HashMap::new(),
        })
    }

    /// Get the cluster ID.
    pub fn cluster_id(&self) -> u64 {
        self.cluster_id
    }

    /// Get a unique request ID.
    fn next_request_id(&self) -> u64 {
        self.request_id_counter.fetch_add(1, Ordering::SeqCst)
    }

    /// Get or create a CDC client for a store.
    async fn get_cdc_client(&self, address: &str) -> Result<ChangeDataClient<Channel>> {
        // Check cache first (lock-free read)
        let guard = self.cdc_clients.pin();
        if let Some(client) = guard.get(address) {
            return Ok(client.clone());
        }
        drop(guard);

        // Create new connection using SecurityManager
        let client: ChangeDataClient<Channel> = self
            .security_mgr
            .connect(address, ChangeDataClient::new)
            .await?;

        // Cache the client
        self.cdc_clients
            .pin()
            .insert(address.to_string(), client.clone());

        Ok(client)
    }

    /// Subscribe to all changes in the cluster.
    ///
    /// This creates CDC subscriptions for all regions in the cluster and returns
    /// a stream of change events.
    pub async fn subscribe_all(
        &self,
        options: CdcOptions,
    ) -> Result<BoxStream<'static, Result<CdcEvent>>> {
        // Subscribe to the entire key range (empty start key = beginning)
        self.subscribe_range(BoundRange::range_from(Key::from(vec![])), options)
            .await
    }

    /// Subscribe to changes in a specific key range.
    ///
    /// # Arguments
    ///
    /// * `range` - The key range to subscribe to.
    /// * `options` - CDC options.
    pub async fn subscribe_range(
        &self,
        range: impl Into<BoundRange>,
        options: CdcOptions,
    ) -> Result<BoxStream<'static, Result<CdcEvent>>> {
        let range: BoundRange = range.into();
        let (start_key, end_key) = range.into_keys();
        let start_key: Vec<u8> = start_key.into();
        let end_key: Vec<u8> = end_key.map(Into::into).unwrap_or_default();

        // Get all regions for this range
        let regions = self.get_regions_for_range(&start_key, &end_key).await?;
        info!("Subscribing to {} regions for CDC", regions.len());

        // Create a merged stream from all region subscriptions
        let (tx, rx) = mpsc::unbounded();

        for region in regions {
            // Get the store address for this region's leader
            let store_id = region.get_store_id()?;
            let store = self.pd.clone().get_store(store_id).await?;
            let store_addr = store.address.clone();

            // Calculate the intersection of region range and requested range
            let region_start = if region.region.start_key > start_key {
                region.region.start_key.clone()
            } else {
                start_key.clone()
            };
            let region_end = if !end_key.is_empty() && !region.region.end_key.is_empty() {
                if region.region.end_key < end_key {
                    region.region.end_key.clone()
                } else {
                    end_key.clone()
                }
            } else if region.region.end_key.is_empty() {
                end_key.clone()
            } else {
                region.region.end_key.clone()
            };

            let request = ChangeDataRequest {
                header: Some(Header {
                    cluster_id: self.cluster_id,
                    ticdc_version: CDC_VERSION.to_string(),
                }),
                region_id: region.id(),
                region_epoch: Some(region.region.region_epoch.clone().unwrap_or_default()),
                checkpoint_ts: options.checkpoint_ts,
                start_key: region_start,
                end_key: region_end,
                request_id: self.next_request_id(),
                extra_op: 0,
                kv_api: options.kv_api as i32,
                filter_loop: options.filter_loop,
                request: Some(Request::Register(Register {})),
            };

            // Spawn a task to handle this region's subscription
            let tx = tx.clone();
            let client = self.get_cdc_client(&store_addr).await?;

            tokio::spawn(async move {
                if let Err(e) = Self::handle_region_subscription(client, request, tx.clone()).await
                {
                    warn!("CDC subscription error for region: {:?}", e);
                    let _ = tx.unbounded_send(Err(e));
                }
            });
        }

        Ok(rx.boxed())
    }

    /// Handle a single region's CDC subscription.
    async fn handle_region_subscription(
        mut client: ChangeDataClient<Channel>,
        request: ChangeDataRequest,
        tx: mpsc::UnboundedSender<Result<CdcEvent>>,
    ) -> Result<()> {
        let region_id = request.region_id;
        debug!("Starting CDC subscription for region {}", region_id);

        // Create a channel for sending requests - we send the initial register request
        // and keep the stream open for potential future requests (like NotifyTxnStatus)
        let (req_tx, req_rx) = mpsc::unbounded::<ChangeDataRequest>();

        // Send the initial registration request
        req_tx
            .unbounded_send(request)
            .map_err(|e| crate::Error::StringError(format!("Failed to send CDC request: {}", e)))?;

        // Start the bi-directional stream with the receiver as the request stream
        let response = client.event_feed(req_rx).await?;
        let mut event_stream = response.into_inner();

        // Keep req_tx alive to prevent the stream from closing
        let _req_tx = req_tx;

        // Process events
        while let Some(event_result) = event_stream.next().await {
            match event_result {
                Ok(event) => {
                    let cdc_events = CdcEvent::from_raw(event);
                    for cdc_event in cdc_events {
                        if tx.unbounded_send(Ok(cdc_event)).is_err() {
                            // Receiver dropped, stop processing
                            return Ok(());
                        }
                    }
                }
                Err(e) => {
                    warn!("CDC stream error for region {}: {:?}", region_id, e);
                    let _ = tx.unbounded_send(Err(e.into()));
                    break;
                }
            }
        }

        debug!("CDC subscription ended for region {}", region_id);
        Ok(())
    }

    /// Get all regions that overlap with the given key range.
    async fn get_regions_for_range(
        &self,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<Vec<RegionWithLeader>> {
        let mut regions = Vec::new();
        let mut current_key = start_key.to_vec();

        loop {
            let region = self.pd.clone().get_region(current_key.clone()).await?;
            regions.push(region.clone());

            // Move to the next region
            let region_end = &region.region.end_key;
            if region_end.is_empty() {
                // This region extends to infinity
                break;
            }
            if !end_key.is_empty() && region_end.as_slice() >= end_key {
                // We've covered the requested range
                break;
            }
            current_key = region_end.clone();
        }

        Ok(regions)
    }
}
