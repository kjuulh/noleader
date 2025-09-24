use anyhow::Context;
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

    let mykey = "myleaderkey";

    let mut handles = Vec::new();

    let db_url = &std::env::var("DATABASE_URL").context("DATABASE_URL is missing")?;
    let pool = sqlx::PgPool::connect_lazy(db_url)?;

    let cancel = CancellationToken::new();
    let mut cancelled_resp = Vec::new();

    tokio::spawn({
        let cancel = cancel.clone();

        async move {
            tokio::signal::ctrl_c().await.expect("to receive shutdown");

            cancel.cancel();
        }
    });

    for _ in 0..100 {
        let pool = pool.clone();
        let cancel = cancel.child_token();

        let item_cancellation = CancellationToken::new();
        cancelled_resp.push(item_cancellation.child_token());

        let handle = tokio::spawn(async move {
            let mut leader = noleader::Leader::new_postgres_pool(mykey, pool);

            leader.with_cancellation(cancel);
            let leader_id = leader.leader_id().await.to_string();

            tokio::spawn({
                let leader = leader.clone();
                let leader_id = leader_id.clone();

                async move {
                    tracing::debug!(leader_id, "starting leader");
                    let res = leader.start().await;

                    tracing::warn!("shutting down");

                    item_cancellation.cancel();

                    if let Err(e) = res {
                        tracing::error!("lots failed: {e:?}");
                    }
                }
            });

            loop {
                tokio::time::sleep(std::time::Duration::from_millis(10000)).await;
                match leader.is_leader().await {
                    noleader::Status::Leader => {
                        tracing::info!(leader_id, "is leader");
                    }
                    noleader::Status::Candidate => {
                        //tracing::debug!("is candiate");
                    }
                }
            }

            #[allow(unreachable_code)]
            Ok::<(), anyhow::Error>(())
        });

        handles.push(handle);
    }

    for cancel in cancelled_resp {
        cancel.cancelled().await;
    }

    for handle in handles {
        handle.abort();
    }

    Ok(())
}
