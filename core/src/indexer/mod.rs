#[cfg(test)]
mod postgres_test_wire;
mod process;
mod progress;

pub use progress::{IndexingEventProgressStatus, IndexingEventsProgressState};
use serde::{Deserialize, Serialize};

pub(crate) mod cron_identity;
pub mod cron_scheduler;
mod dependency;
pub mod tables;
#[cfg(test)]
mod tables_atomic_tests;
pub use dependency::ContractEventDependenciesMapFromRelationshipsError;
mod fetch_logs;
pub use fetch_logs::FetchLogsResult;
mod heartbeat;
mod last_synced;
pub(crate) use last_synced::verify_versioned_cron_certificate;
pub mod native_transfer;
pub mod no_code;
pub mod reorg;
pub mod schema;
pub mod start;
pub mod task_tracker;
pub mod tip_logs;

pub use dependency::{ContractEventDependencies, EventDependencies, EventsDependencyTree};

use crate::manifest::{contract::Contract, native_transfer::NativeTransfers};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Indexer {
    pub name: String,

    pub contracts: Vec<Contract>,

    pub native_transfers: NativeTransfers,
}

mod required_delivery;
