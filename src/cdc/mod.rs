// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

//! CDC (Change Data Capture) client for TiKV.
//!
//! This module provides a client for subscribing to change events from TiKV.
//! It wraps the low-level gRPC CDC protocol and provides a high-level streaming API.
//!
//! # Example
//!
//! ```rust,no_run
//! # use tikv_client::{CdcClient, Result};
//! # use futures::prelude::*;
//! # fn main() -> Result<()> {
//! # futures::executor::block_on(async {
//! let cdc_client = CdcClient::new(vec!["127.0.0.1:2379"]).await?;
//! let mut stream = cdc_client.subscribe_all().await?;
//! while let Some(event) = stream.next().await {
//!     println!("Change event: {:?}", event);
//! }
//! # Ok(())
//! # })}
//! ```

mod client;
mod event;

pub use client::{CdcClient, CdcOptions};
pub use event::{CdcError, CdcEvent, RowChange, RowOp};
