//! Checkpoint publication boundary, separate from concurrent callback execution.
use crate::indexer::reorg::ReorgSubscription;
use alloy::primitives::U64;
use std::future::Future;
use tokio::sync::oneshot;

#[derive(Default)]
pub(super) struct CallbackCheckpoints {
    previous: Option<oneshot::Receiver<()>>,
    generation: u64,
    recovery: Option<ReorgSubscription>,
}

pub(super) struct CheckpointTurn {
    previous: Option<oneshot::Receiver<()>>,
    complete: oneshot::Sender<()>,
    recovery: Option<ReorgSubscription>,
}

impl CallbackCheckpoints {
    pub(super) fn with_recovery(recovery: ReorgSubscription) -> Self {
        Self { recovery: Some(recovery), ..Self::default() }
    }

    pub(super) fn for_generation(&mut self, generation: u64) {
        if self.generation != generation {
            // The exclusive rollback barrier drained old callbacks. Replacement
            // work starts a new ordering chain, independent of cancelled old turns.
            self.previous = None;
            self.generation = generation;
        }
    }

    pub(super) fn next(&mut self) -> CheckpointTurn {
        let (complete, receiver) = oneshot::channel();
        let previous = self.previous.replace(receiver);
        CheckpointTurn { previous, complete, recovery: self.recovery.clone() }
    }
}

impl CheckpointTurn {
    pub(super) async fn commit(
        self,
        through: U64,
        update: impl Future<Output = ()>,
    ) -> Result<(), ()> {
        if let Some(previous) = self.previous {
            // A dropped predecessor never authorizes cursor advancement. Dropping
            // this turn's sender propagates the same gap to later checkpoints.
            previous.await.map_err(|_| ())?;
        }
        update.await;
        if let Some(recovery) = self.recovery {
            recovery.completed_through(through.to::<u64>());
        }
        // A stream may end without requesting another turn.
        let _ = self.complete.send(());
        Ok(())
    }
}
