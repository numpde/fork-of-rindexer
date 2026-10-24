//! Registry-owned failure records; health receives only stream identity, never callback error text.
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Mutex};
use tokio::sync::Notify;

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
    reason: String,
}

#[derive(Default)]
pub(crate) struct CallbackFailures {
    streams: Mutex<HashMap<(String, String), RecordedFailure>>,
    changed: Notify,
}

impl CallbackFailures {
    pub(crate) fn record(&self, stream: FailedStream, reason: String) {
        let mut streams = self.streams.lock().expect("callback failure state lock poisoned");
        let key = (stream.id.clone(), stream.network.clone());
        if streams.contains_key(&key) {
            return;
        }
        streams.insert(key, RecordedFailure { stream, reason });
        drop(streams);
        self.changed.notify_waiters();
    }

    pub(crate) fn snapshot(&self) -> Vec<FailedStream> {
        self.streams
            .lock()
            .expect("callback failure state lock poisoned")
            .values()
            .map(|failure| failure.stream.clone())
            .collect()
    }

    pub(crate) fn reason(&self, id: &str, network: &str) -> Option<String> {
        self.streams
            .lock()
            .expect("callback failure state lock poisoned")
            .get(&(id.to_string(), network.to_string()))
            .map(|failure| failure.reason.clone())
    }

    pub(crate) async fn wait_failed(&self, id: &str, network: &str) -> String {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(reason) = self.reason(id, network) {
                return reason;
            }
            notified.await;
        }
    }
}
