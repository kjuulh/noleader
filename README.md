# noleader

A small, ergonomic Rust crate for leader election using NATS KeyValue (KV) as the distributed locking mechanism.
Does not require a fixed set of nodes—any number of candidates can join or drop out dynamically.

## Disclaimer

This library is still young and the API is subject to change.

## Features

* **Distributed locking with TTL**
  Uses NATS JetStream KV store to create a key with a TTL, ensuring only one leader at a time.  
* **Automatic renewal with jitter**
  Renews leadership every \~10 s (plus a random back-off) to avoid thundering‐herd elections.
* **Graceful shutdown**
  Integrates with `tokio_util::sync::CancellationToken` so you can cancel leadership and ongoing work cleanly.
* **Leader-only work loop**
  `do_while_leader` runs your async closure as long as you hold leadership, cancelling it immediately upon relinquish.
* **No required cluster size**
  Nodes can join or leave at any time—no minimum/maximum constraints.

## Intended use-case

Noleader is not built for distributed consensus, or fast re-election procedures. It take upwards to a minute to get re-elected, state is the users responsibility to handle.

Noleader is pretty much just a distributed lock, intended for use-cases where the user wants to only have a single node scheduling work etc.

Good alternatives are:

- Raft (for distributed consensus)
- Relational databases (for locking)

## Installation

```toml
[dependencies]
noleader = "0.1"
```

Then in your code:

```rust
use noleader::Leader;
```

## Quick Example

```rust
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Set up logging
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive("noleader=debug".parse().unwrap())
                .add_directive("info".parse().unwrap()),
        )
        .init();

    let bucket = "my_bucket";
    let key    = "election_key";
    let client = async_nats::connect("localhost:4222").await?;

    // Create a new leader election instance
    let leader = Leader::new(bucket, key, client.clone());

    // Ensure the KV bucket exists
    leader.create_bucket().await?;

    // Attempts to acquire election loop, will call inner function if it wins, if it loses it will retry over again.
    // Will block until either the inner function returns and error, or the election processes crashes, intended to allow the application to properly restart
    leader
        .acquire_and_run({
            move |token| {
                let leader_id = leader_id.clone();

                async move {
                    loop {
                        if token.is_cancelled() {
                            return Ok(());
                        }

                        tracing::info!(leader_id, "do work as leader");
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
            }
        })
        .await?;


    Ok(())
}
```

## Examples

See the examples folder in ./crates/noleader/examples

## Architecture

Noleader uses a simple election stealing 


## License

This crate is licensed under the same terms as the workspace (MIT or Apache-2.0).
See [LICENSE-MIT](../LICENSE-MIT) and [LICENSE-APACHE](../LICENSE-APACHE).

## Contribute

Issues and PRs are welcome!
Repository: [https://git.front.kjuulh.io/kjuulh/noleader](https://git.front.kjuulh.io/kjuulh/noleader)
Development happens publicly on the `main` branch—feel free to fork and send a merge request.

