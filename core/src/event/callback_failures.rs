//! Registry-owned failure records; health receives only stream identity, never callback error text.
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Mutex};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FailedStream {
    pub id: String,
    pub indexer: String,
    pub contract: String,
    pub event: String,
    pub network: String,
    pub from_block: u64,
    pub to_block: u64,
}

struct RecordedFailure {
    stream: FailedStream,
    #[cfg(test)]
    reason: String,
}

#[derive(Default)]
pub(crate) struct CallbackFailures {
    streams: Mutex<HashMap<(String, String), RecordedFailure>>,
}

impl CallbackFailures {
    #[cfg(test)]
    pub(crate) fn record(&self, stream: FailedStream, reason: String) {
        self.streams
            .lock()
            .expect("callback failure state lock poisoned")
            .entry((stream.id.clone(), stream.network.clone()))
            .or_insert(RecordedFailure { stream, reason });
    }

    pub(crate) fn snapshot(&self) -> Vec<FailedStream> {
        self.streams
            .lock()
            .expect("callback failure state lock poisoned")
            .values()
            .map(|failure| failure.stream.clone())
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn reason(&self, id: &str, network: &str) -> Option<String> {
        self.streams
            .lock()
            .expect("callback failure state lock poisoned")
            .get(&(id.to_string(), network.to_string()))
            .map(|failure| failure.reason.clone())
    }
}
