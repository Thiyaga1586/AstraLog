//! Bounded, fair selection for the P6 group and offset coordinator actors.
//! Both workloads have separate bounded queues. A biased control-first select
//! can otherwise indefinitely starve durable mutations under recovery load.

use tokio::sync::{mpsc, oneshot};

const MAX_CONTROL_BURST: u8 = 8;

pub(crate) enum Queued<C, M> {
    Control(C),
    Mutation(M),
}

/// Preserve shutdown priority and serve at most eight consecutive control
/// commands when a mutation is queued. This guarantees scheduling progress,
/// not successful mutation: quorum and fencing checks remain unchanged.
pub(crate) async fn next<C, M>(
    shutdown: &mut oneshot::Receiver<()>,
    control: &mut mpsc::Receiver<C>,
    mutation: &mut mpsc::Receiver<M>,
    consecutive_controls: &mut u8,
) -> Option<Queued<C, M>> {
    let yield_to_mutation = *consecutive_controls >= MAX_CONTROL_BURST && !mutation.is_empty();
    tokio::select! {
        biased;
        _ = shutdown => None,
        command = control.recv(), if !yield_to_mutation => {
            command.map(|command| {
                *consecutive_controls = consecutive_controls.saturating_add(1);
                Queued::Control(command)
            })
        }
        command = mutation.recv() => {
            command.map(|command| {
                *consecutive_controls = 0;
                Queued::Mutation(command)
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn queued_mutation_is_served_after_at_most_eight_controls() {
        let (control_tx, mut control_rx) = mpsc::channel(64);
        let (mutation_tx, mut mutation_rx) = mpsc::channel(2);
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        for command in 0..64_u8 {
            control_tx.try_send(command).unwrap();
        }
        mutation_tx.try_send(99_u8).unwrap();
        let mut controls = 0;
        for expected in 0..MAX_CONTROL_BURST {
            match next(
                &mut shutdown_rx,
                &mut control_rx,
                &mut mutation_rx,
                &mut controls,
            )
            .await
            {
                Some(Queued::Control(actual)) => assert_eq!(actual, expected),
                _ => panic!("control priority did not admit the initial bounded burst"),
            }
        }
        match next(
            &mut shutdown_rx,
            &mut control_rx,
            &mut mutation_rx,
            &mut controls,
        )
        .await
        {
            Some(Queued::Mutation(99)) => assert_eq!(controls, 0),
            _ => panic!("pending durable mutation starved behind control backlog"),
        }
        // No mutation pending: control traffic must remain serviceable.
        assert!(matches!(
            next(
                &mut shutdown_rx,
                &mut control_rx,
                &mut mutation_rx,
                &mut controls
            )
            .await,
            Some(Queued::Control(8))
        ));
        shutdown_tx.send(()).unwrap();
        assert!(
            next(
                &mut shutdown_rx,
                &mut control_rx,
                &mut mutation_rx,
                &mut controls
            )
            .await
            .is_none()
        );
    }
}
