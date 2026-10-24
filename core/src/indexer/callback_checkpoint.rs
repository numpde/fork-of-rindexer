//! Checkpoint publication boundary, separate from concurrent callback execution.
use std::future::Future;

#[derive(Default)]
pub(super) struct CallbackCheckpoints {}

pub(super) struct CheckpointTurn;

impl CallbackCheckpoints {
    pub(super) fn next(&mut self) -> CheckpointTurn {
        CheckpointTurn
    }
}

impl CheckpointTurn {
    pub(super) async fn commit(self, update: impl Future<Output = ()>) -> Result<(), ()> {
        update.await;
        Ok(())
    }
}
