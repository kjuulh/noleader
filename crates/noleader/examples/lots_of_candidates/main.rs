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

    let mut handles = Vec::new();

    for _ in 0..100 {
        let client = client.clone();

        let handle = tokio::spawn(async move {
            let leader = noleader::Leader::new_nats(mykey, mybucket, client);
            let leader_id = leader.leader_id().await.to_string();

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

    for handle in handles {
        handle.await??;
    }

    Ok(())
}
