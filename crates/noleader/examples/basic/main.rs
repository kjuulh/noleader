use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Set up logger
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive("noleader=debug".parse().unwrap())
                .add_directive("lots_of_candidates=debug".parse().unwrap())
                .add_directive("info".parse().unwrap()),
        )
        .init();

    let mybucket = "mytestbucket";
    let mykey = "myleaderkey";
    let client = async_nats::connect("localhost:4222").await?;

    let leader = noleader::Leader::new(mybucket, mykey, client);
    let leader_id = leader.leader_id().await.to_string();

    tracing::info!("creating bucket");
    leader.create_bucket().await?;

    tokio::spawn({
        let leader = leader.clone();
        let leader_id = leader_id.clone();

        async move {
            tracing::debug!(leader_id, "starting leader");
            leader
                .start(CancellationToken::default())
                .await
                .expect("to succeed");
        }
    });

    leader
        .do_while_leader(move |token| async move {
            loop {
                if token.is_cancelled() {
                    return Ok(());
                }

                tracing::info!("do work as leader");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        })
        .await?;

    Ok(())
}
