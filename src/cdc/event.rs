// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

//! High-level CDC event types.

use crate::Key;
use crate::proto::cdcpb;

/// The type of row operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowOp {
    /// A key-value pair was inserted or updated.
    Put,
    /// A key was deleted.
    Delete,
    /// Unknown operation type.
    Unknown,
}

impl From<cdcpb::event::row::OpType> for RowOp {
    fn from(op: cdcpb::event::row::OpType) -> Self {
        match op {
            cdcpb::event::row::OpType::Put => RowOp::Put,
            cdcpb::event::row::OpType::Delete => RowOp::Delete,
            cdcpb::event::row::OpType::Unknown => RowOp::Unknown,
        }
    }
}

impl From<i32> for RowOp {
    fn from(op: i32) -> Self {
        match cdcpb::event::row::OpType::try_from(op) {
            Ok(op) => op.into(),
            Err(_) => RowOp::Unknown,
        }
    }
}

/// A single row change event.
#[derive(Debug, Clone)]
pub struct RowChange {
    /// The key that changed.
    pub key: Key,
    /// The new value (for Put operations).
    pub value: Option<Vec<u8>>,
    /// The old value (if available, for updates/deletes).
    pub old_value: Option<Vec<u8>>,
    /// The type of operation.
    pub op: RowOp,
    /// The start timestamp of the transaction.
    pub start_ts: u64,
    /// The commit timestamp of the transaction.
    pub commit_ts: u64,
}

impl From<cdcpb::event::Row> for RowChange {
    fn from(row: cdcpb::event::Row) -> Self {
        let op = RowOp::from(row.op_type);
        RowChange {
            key: row.key.into(),
            value: if row.value.is_empty() {
                None
            } else {
                Some(row.value)
            },
            old_value: if row.old_value.is_empty() {
                None
            } else {
                Some(row.old_value)
            },
            op,
            start_ts: row.start_ts,
            commit_ts: row.commit_ts,
        }
    }
}

/// A CDC event from TiKV.
#[derive(Debug, Clone)]
pub enum CdcEvent {
    /// One or more row changes.
    Rows {
        /// The region ID this event belongs to.
        region_id: u64,
        /// The row changes.
        rows: Vec<RowChange>,
    },
    /// A resolved timestamp - all events before this timestamp have been sent.
    ResolvedTs {
        /// The regions this resolved timestamp applies to.
        regions: Vec<u64>,
        /// The resolved timestamp.
        ts: u64,
    },
    /// An error occurred for a region.
    Error {
        /// The region ID.
        region_id: u64,
        /// The error.
        error: CdcError,
    },
    /// An admin event (region split, merge, etc.).
    Admin {
        /// The region ID.
        region_id: u64,
    },
}

/// A CDC error.
#[derive(Debug, Clone)]
pub enum CdcError {
    /// The region's leader changed.
    NotLeader {
        /// The new leader's store ID, if known.
        leader_store_id: Option<u64>,
    },
    /// The region was not found.
    RegionNotFound,
    /// The region epoch doesn't match.
    EpochNotMatch,
    /// Duplicate request.
    DuplicateRequest,
    /// Server is busy.
    ServerIsBusy,
    /// Cluster ID mismatch.
    ClusterIdMismatch { current: u64, request: u64 },
    /// Compatibility error - version mismatch.
    Compatibility { required_version: String },
    /// Other error.
    Other(String),
}

impl std::fmt::Display for CdcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CdcError::NotLeader { leader_store_id } => {
                write!(f, "NotLeader(new_leader={:?})", leader_store_id)
            }
            CdcError::RegionNotFound => write!(f, "RegionNotFound"),
            CdcError::EpochNotMatch => write!(f, "EpochNotMatch"),
            CdcError::DuplicateRequest => write!(f, "DuplicateRequest"),
            CdcError::ServerIsBusy => write!(f, "ServerIsBusy"),
            CdcError::ClusterIdMismatch { current, request } => {
                write!(
                    f,
                    "ClusterIdMismatch(current={}, request={})",
                    current, request
                )
            }
            CdcError::Compatibility { required_version } => {
                write!(f, "Compatibility(required={})", required_version)
            }
            CdcError::Other(msg) => write!(f, "Other({})", msg),
        }
    }
}

impl From<cdcpb::Error> for CdcError {
    fn from(err: cdcpb::Error) -> Self {
        if let Some(not_leader) = err.not_leader {
            return CdcError::NotLeader {
                leader_store_id: not_leader.leader.map(|l| l.store_id),
            };
        }
        if err.region_not_found.is_some() {
            return CdcError::RegionNotFound;
        }
        if err.epoch_not_match.is_some() {
            return CdcError::EpochNotMatch;
        }
        if err.duplicate_request.is_some() {
            return CdcError::DuplicateRequest;
        }
        if err.server_is_busy.is_some() {
            return CdcError::ServerIsBusy;
        }
        if let Some(mismatch) = err.cluster_id_mismatch {
            return CdcError::ClusterIdMismatch {
                current: mismatch.current,
                request: mismatch.request,
            };
        }
        if let Some(compat) = err.compatibility {
            return CdcError::Compatibility {
                required_version: compat.required_version,
            };
        }
        CdcError::Other(format!("Unknown CDC error: {:?}", err))
    }
}

impl CdcEvent {
    /// Create CDC events from a raw ChangeDataEvent.
    pub fn from_raw(event: cdcpb::ChangeDataEvent) -> Vec<Self> {
        let mut result = Vec::new();

        // Process resolved timestamp
        if let Some(resolved_ts) = event.resolved_ts {
            result.push(CdcEvent::ResolvedTs {
                regions: resolved_ts.regions,
                ts: resolved_ts.ts,
            });
        }

        // Process events
        for ev in event.events {
            match ev.event {
                Some(cdcpb::event::Event::Entries(entries)) => {
                    // Filter to only committed entries
                    let rows: Vec<RowChange> = entries
                        .entries
                        .into_iter()
                        .filter(|row| {
                            // Only include committed rows
                            row.r#type == cdcpb::event::LogType::Committed as i32
                        })
                        .map(RowChange::from)
                        .collect();

                    if !rows.is_empty() {
                        result.push(CdcEvent::Rows {
                            region_id: ev.region_id,
                            rows,
                        });
                    }
                }
                Some(cdcpb::event::Event::Error(error)) => {
                    result.push(CdcEvent::Error {
                        region_id: ev.region_id,
                        error: error.into(),
                    });
                }
                Some(cdcpb::event::Event::Admin(_)) => {
                    result.push(CdcEvent::Admin {
                        region_id: ev.region_id,
                    });
                }
                Some(cdcpb::event::Event::ResolvedTs(ts)) => {
                    // Deprecated field, but handle it anyway
                    result.push(CdcEvent::ResolvedTs {
                        regions: vec![ev.region_id],
                        ts,
                    });
                }
                Some(cdcpb::event::Event::LongTxn(_)) => {
                    // Long transaction info - we can ignore this for now
                }
                None => {}
            }
        }

        result
    }
}
