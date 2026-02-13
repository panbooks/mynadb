//! Parallel fan-out utility for broadcasting requests to data nodes.

use futures::future::{join_all, BoxFuture};

/// Fan out async operations to data nodes in parallel, collecting results and errors.
///
/// Takes a vec of `(node_id, future)` pairs and runs all futures concurrently.
/// Returns `(successes, errors)` where successes include the node_id for per-node tracking
/// and errors are formatted as `"{node_id}: {error}"`.
pub async fn fan_out<T: Send>(
    futures: Vec<(String, BoxFuture<'_, Result<T, String>>)>,
) -> (Vec<(String, T)>, Vec<String>) {
    let (node_ids, futs): (Vec<_>, Vec<_>) = futures.into_iter().unzip();

    let results = join_all(futs).await;

    let mut successes = Vec::new();
    let mut errors = Vec::new();

    for (node_id, result) in node_ids.into_iter().zip(results) {
        match result {
            Ok(value) => successes.push((node_id, value)),
            Err(e) => errors.push(format!("{}: {}", node_id, e)),
        }
    }

    (successes, errors)
}
