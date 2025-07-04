use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::Context;
use async_nats::jetstream::kv;
use rand::Rng;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Clone)]
pub struct Leader {
    shutting_down: Arc<AtomicBool>,
    is_leader: Arc<AtomicBool>,
    inner: Arc<RwLock<InnerLeader>>,
}
const DEFAULT_INTERVAL: Duration = std::time::Duration::from_secs(10);

impl Leader {
    pub fn new(bucket: &str, key: &str, client: async_nats::Client) -> Self {
        Self {
            shutting_down: Arc::new(AtomicBool::new(false)),
            is_leader: Arc::new(AtomicBool::new(false)),
            inner: Arc::new(RwLock::new(InnerLeader::new(bucket, key, client))),
        }
    }

    pub async fn acquire_and_run<F, Fut>(&self, f: F) -> anyhow::Result<()>
    where
        F: Fn(CancellationToken) -> Fut,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let parent_token = CancellationToken::default();
        let s = self.clone();

        let server_token = parent_token.child_token();

        // Start the server election process in another task, this is because start is blocking
        let handle = tokio::spawn({
            let server_token = server_token.child_token();
            async move {
                match s.start(server_token).await {
                    Ok(_) => {}
                    Err(e) => tracing::error!("leader election process failed: {}", e),
                }

                tracing::info!("shutting down noleader");

                parent_token.cancel();
            }
        });

        // Do the work if we're leader
        let res = self
            .do_while_leader_inner(server_token.child_token(), f)
            .await;

        // Stop the server election process if our provided functions returns an error;
        server_token.cancel();
        // Close down the task as well, it should already be stopped, but this forces the task to close
        handle.abort();
        res?;

        Ok(())
    }

    pub async fn do_while_leader<F, Fut>(&self, f: F) -> anyhow::Result<()>
    where
        F: Fn(CancellationToken) -> Fut,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        self.do_while_leader_inner(CancellationToken::new(), f)
            .await
    }

    async fn do_while_leader_inner<F, Fut>(
        &self,
        cancellation_token: CancellationToken,
        f: F,
    ) -> anyhow::Result<()>
    where
        F: Fn(CancellationToken) -> Fut,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        loop {
            let cancellation_token = cancellation_token.child_token();

            let is_leader = self.is_leader.clone();
            if !is_leader.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }

            let child_token = cancellation_token.child_token();

            let guard = tokio::spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

                    if !is_leader.load(Ordering::Relaxed) {
                        cancellation_token.cancel();
                    }
                }
            });

            let res = f(child_token).await;
            guard.abort();
            res?;
        }
    }

    pub async fn leader_id(&self) -> Uuid {
        let inner = self.inner.read().await;
        inner.id
    }

    pub async fn create_bucket(&self) -> anyhow::Result<()> {
        let mut inner = self.inner.write().await;
        tracing::info!("creating bucket leadership bucket");

        inner.create_bucket().await?;

        Ok(())
    }

    pub async fn start(&self, cancellation_token: CancellationToken) -> anyhow::Result<()> {
        let mut attempts = 1;

        // Initial attempt
        let _ = self.try_become_leader().await;

        loop {
            let wait_factor = {
                let mut rng = rand::rng();
                rng.random_range(0.001..1.000)
            };

            let sleep_fut = tokio::time::sleep((DEFAULT_INTERVAL * attempts).mul_f64(wait_factor));

            tokio::select! {
                _ = sleep_fut => {},
                _ = cancellation_token.cancelled() => {
                    self.shutting_down.store(true, std::sync::atomic::Ordering::Relaxed); // Ordering can be relaxed, because our operation is an atomic update
                    return Ok(())
                }
            };

            match self.try_become_leader().await {
                Ok(_) => {
                    self.is_leader
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                    attempts = 1;
                }
                Err(e) => {
                    tracing::debug!(error = e.to_string(), "failed to become leader");
                    self.is_leader.store(false, Ordering::Relaxed);
                    if attempts <= 10 {
                        attempts += 1;
                    }
                }
            }
        }
    }

    async fn try_become_leader(&self) -> anyhow::Result<()> {
        let mut inner = self.inner.write().await;
        match inner.start().await {
            Ok(_) => Ok(()),
            Err(e) => {
                tracing::trace!("failed to update leadership status: {:#?}", e);
                anyhow::bail!("{}", e);
            }
        }
    }

    pub async fn is_leader(&self) -> Status {
        if self
            .shutting_down
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return Status::Candidate;
        }

        if self.is_leader.load(Ordering::Relaxed) {
            Status::Leader
        } else {
            Status::Candidate
        }
    }
}

pub enum Status {
    Leader,
    Candidate,
}

struct InnerLeader {
    state: LeaderState,

    bucket: String,
    key: String,

    id: uuid::Uuid,
    revision: u64,

    client: async_nats::jetstream::Context,
}

#[derive(Default, Clone)]
enum LeaderState {
    #[default]
    Unknown,
    Leader,
    Campaigning,
}

impl InnerLeader {
    pub fn new(bucket: &str, key: &str, client: async_nats::Client) -> Self {
        Self {
            bucket: bucket.into(),
            key: key.into(),

            id: uuid::Uuid::new_v4(),
            revision: u64::MIN,

            state: LeaderState::Unknown,
            client: async_nats::jetstream::new(client),
        }
    }

    pub async fn create_bucket(&mut self) -> anyhow::Result<()> {
        if (self.client.get_key_value(&self.bucket).await).is_ok() {
            return Ok(());
        }

        if let Err(e) = self
            .client
            .create_key_value(kv::Config {
                bucket: self.bucket.clone(),
                description: "leadership bucket for noleader".into(),
                limit_markers: Some(std::time::Duration::from_secs(60)),
                max_age: std::time::Duration::from_secs(60),
                ..Default::default()
            })
            .await
        {
            tracing::info!(
                "bucket creation failed, it might have just been a conflict, testing again: {e}"
            );

            if (self.client.get_key_value(&self.bucket).await).is_ok() {
                return Ok(());
            }

            anyhow::bail!("failed to create bucket: {}", e)
        }

        Ok(())
    }

    /// start, will run a blocking operation for becoming the next leader.
    pub async fn start(&mut self) -> anyhow::Result<()> {
        // Attempt to grab leadership,
        // If leader, do updates as long as their key is the value

        match self.state {
            LeaderState::Unknown => {
                tracing::debug!("state is unknown, trying to become leader");
                // start campaigning
                self.state = LeaderState::Campaigning;

                self.try_for_leadership().await?;
            }

            LeaderState::Campaigning => {
                tracing::debug!("campaigning for leadership");

                self.try_for_leadership().await?;
            }
            LeaderState::Leader => {
                tracing::debug!("updating leadership");
                // maintain leadership
                match self.update_leadership().await {
                    Ok(_) => {}
                    Err(e) => {
                        self.state = LeaderState::Unknown;
                        anyhow::bail!("failed to update leadership: {}", e);
                    }
                }

                return Ok(());
            }
        }

        Ok(())
    }

    async fn update_leadership(&mut self) -> anyhow::Result<()> {
        let bucket = self.client.get_key_value(&self.bucket).await?;

        let Some(val) = bucket.get(&self.key).await? else {
            anyhow::bail!("key doesn't exists, we've lost leadership status")
        };

        let Ok(id) = uuid::Uuid::from_slice(&val) else {
            anyhow::bail!("value has changed, it is no longer a uuid, dropping leadership status");
        };

        if id != self.id {
            anyhow::bail!("leadership has changed")
        }

        let rev = bucket
            .update(
                &self.key,
                bytes::Bytes::copy_from_slice(self.id.as_bytes()),
                self.revision,
            )
            .await?;

        self.revision = rev;

        Ok(())
    }

    async fn try_for_leadership(&mut self) -> anyhow::Result<()> {
        let bucket = self
            .client
            .get_key_value(&self.bucket)
            .await
            .context("failed to get bucket")?;

        let rev = match bucket
            .create_with_ttl(
                &self.key,
                bytes::Bytes::copy_from_slice(self.id.as_bytes()),
                std::time::Duration::from_secs(60),
            )
            .await
        {
            Ok(rev) => rev,
            Err(e) => match e.kind() {
                kv::CreateErrorKind::AlreadyExists => {
                    anyhow::bail!("another candidate has leadership status")
                }
                _ => {
                    anyhow::bail!("{}", e);
                }
            },
        };

        self.revision = rev;

        tokio::time::sleep(DEFAULT_INTERVAL).await;

        tracing::info!("acquired leadership status");

        let leadership_state = self.leadership_status().await?;

        if !leadership_state.is_leader(&self.id) {
            anyhow::bail!("failed to become leader, there is likely some churn going on");
        }

        // We're a leader
        self.state = LeaderState::Leader;

        Ok(())
    }

    async fn leadership_status(&mut self) -> anyhow::Result<LeadershipState> {
        let bucket = self.client.get_key_value(&self.bucket).await?;

        let val = bucket.get(&self.key).await?;

        Ok(match val {
            Some(content) => {
                let id = match uuid::Uuid::from_slice(&content) {
                    Ok(u) => u,
                    Err(e) => {
                        tracing::warn!(
                            "leadership state is not a valid UUID, ignoring the value: {}",
                            e
                        );
                        return Ok(LeadershipState::NotFound);
                    }
                };

                LeadershipState::Allocated { id }
            }
            None => LeadershipState::NotFound,
        })
    }
}

enum LeadershipState {
    NotFound,
    Allocated { id: uuid::Uuid },
}

impl LeadershipState {
    pub fn is_leader(&self, leader_id: &Uuid) -> bool {
        match self {
            LeadershipState::Allocated { id } => id == leader_id,
            _ => false,
        }
    }
}
