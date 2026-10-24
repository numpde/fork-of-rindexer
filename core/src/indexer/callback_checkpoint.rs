//! Checkpoint publication boundary, separate from concurrent callback execution.
use std::future::Future;
use tokio::sync::oneshot;

#[derive(Default)]
pub(super) struct CallbackCheckpoints {
    previous: Option<oneshot::Receiver<()>>,
    generation: u64,
}

pub(super) struct CheckpointTurn {
    previous: Option<oneshot::Receiver<()>>,
    complete: oneshot::Sender<()>,
}

impl CallbackCheckpoints {
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
        CheckpointTurn { previous, complete }
    }
}

impl CheckpointTurn {
    pub(super) async fn commit(self, update: impl Future<Output = ()>) -> Result<(), ()> {
        if let Some(previous) = self.previous {
            // A dropped predecessor never authorizes cursor advancement. Dropping
            // this turn's sender propagates the same gap to later checkpoints.
            previous.await.map_err(|_| ())?;
        }
        update.await;
        // A stream may end without requesting another turn.
        let _ = self.complete.send(());
        Ok(())
    }
}
