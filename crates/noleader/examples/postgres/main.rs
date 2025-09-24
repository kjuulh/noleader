use anyhow::Context;
use tokio::signal;
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

    let mykey = "postgres";

    let mut leader = noleader::Leader::new_postgres(
        mykey,
        &std::env::var("DATABASE_URL").context("DATABASE_URL is missing")?,
    );
    leader.with_cancel_task(async move {
        signal::ctrl_c().await.unwrap();
    });

    let leader_id = leader.leader_id().await.to_string();

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
