use std::time::{Duration, Instant};

use futures::future::Future;

/// Run a future, and call the provided function at the specified intervals until the future completes
pub(crate) async fn run_future_with_interval<F, O>(
    mut funk: impl FnMut(Duration),
    duration: Duration,
    future: F,
) -> O
where
    F: Future<Output = O>,
{
    let start = Instant::now();
    futures::pin_mut!(future);

    loop {
        tokio::select! {
            result = &mut future => {
                return result;
            }
            _ = tokio::time::delay_for(duration) => {
                funk(start.elapsed());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_works_correctly() {
        let mut count = 0;

        let return_value = run_future_with_interval(
            |_| {
                count += 1;
            },
            Duration::from_millis(250),
            async {
                tokio::time::delay_for(Duration::from_millis(1050)).await ;

                24
            },
        )
        .await;

        assert_eq!(return_value, 24);
        assert_eq!(count, 4);
    }
}
