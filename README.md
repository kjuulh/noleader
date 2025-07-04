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

Noleader is not built for distributed consensus, or fast re-election produces. It take upwards to a minute to get reelected, state is the users responsibility to handle.

Noleader is pretty much just a distributed lock, intended for use-cases where the use wants to only have a single node scheduling work etc.

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

    // Spawn the election loop
    tokio::spawn({
        let leader = leader.clone();
        async move {
            leader
                .start(CancellationToken::default())
                .await
                .expect("leadership loop failed");
        }
    });

    // Do work while we are the leader
    leader
        .do_while_leader(|cancel_token| async move {
            loop {
                if cancel_token.is_cancelled() {
                    break;
                }
                tracing::info!("🔑 I am the leader—doing work");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
            Ok(())
        })
        .await?;

    Ok(())
}
```

## API Overview

* **`Leader::new(bucket: &str, key: &str, client: async_nats::Client) -> Leader`**
  Create a new election participant.
* **`create_bucket(&self) -> anyhow::Result<()>`**
  Ensures the KV bucket exists (no-op if already created).
* **`start(&self, token: CancellationToken) -> anyhow::Result<()>`**
  Begins the background leader-election loop; renews TTL on success or retries on failure.
* **`do_while_leader<F, Fut>(&self, f: F) -> anyhow::Result<()>`**
  Runs your closure as long as you hold leadership; cancels immediately on loss.
* **`leader_id(&self) -> Uuid`**
  Returns your unique candidate ID.
* **`is_leader(&self) -> Status`**
  Returns `Status::Leader` or `Status::Candidate`, taking shutdown into account.

### Types

```rust
pub enum Status {
    Leader,
    Candidate,
}
```

## License

This crate is licensed under the same terms as the workspace (MIT or Apache-2.0).
See [LICENSE-MIT](../LICENSE-MIT) and [LICENSE-APACHE](../LICENSE-APACHE).

## Contribute

Issues and PRs are welcome!
Repository: [https://git.front.kjuulh.io/kjuulh/noleader](https://git.front.kjuulh.io/kjuulh/noleader)
Development happens publicly on the `main` branch—feel free to fork and send a merge request.

