use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::Context;
use rand::Rng;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::backend::{Backend, Key, LeaderId};

#[derive(Clone)]
pub struct Leader {
    shutting_down: Arc<AtomicBool>,
    is_leader: Arc<AtomicBool>,
    inner: Arc<RwLock<InnerLeader>>,
}
const DEFAULT_INTERVAL: Duration = std::time::Duration::from_secs(10);

mod backend;

impl Leader {
    pub fn new(key: &str, backend: Backend) -> Self {
        Self {
            shutting_down: Arc::new(AtomicBool::new(false)),
            is_leader: Arc::new(AtomicBool::new(false)),
            inner: Arc::new(RwLock::new(InnerLeader::new(backend, key))),
        }
    }

    pub fn new_nats(key: &str, bucket: &str, client: async_nats::Client) -> Self {
        Self::new(key, Backend::nats(client, bucket))
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
                    tokio::select! {
                        _ = tokio::time::sleep(std::time::Duration::from_millis(500)) => {}
                        _ = cancellation_token.cancelled() => {
                            break;
                        }

                    }

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
        inner.leader_id.clone().into()
    }

    pub async fn start(&self, cancellation_token: CancellationToken) -> anyhow::Result<()> {
        let mut attempts = 1;

        {
            self.inner.write().await.backend.setup().await?;
        }

        // Initial attempt
        let _ = self.try_become_leader().await;

        loop {
            let wait_factor = {
                let mut rng = rand::rng();
                rng.random_range(0.50..1.00)
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
                    self.is_leader.store(true, Ordering::Relaxed);
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

    backend: Backend,

    key: Key,
    leader_id: LeaderId,
    revision: u64,
}

#[derive(Default, Clone)]
enum LeaderState {
    #[default]
    Unknown,
    Leader,
    Campaigning,
}

impl InnerLeader {
    pub fn new(backend: Backend, key: impl Into<Key>) -> Self {
        Self {
            backend,
            leader_id: LeaderId::new(),
            revision: u64::MIN,

            key: key.into(),

            state: LeaderState::Unknown,
        }
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
        let val = self
            .backend
            .get(&self.key)
            .await
            .context("could not find key, we've lost leadership status")?;

        match val {
            backend::LeaderValue::Unknown => anyhow::bail!("leadership is unknown"),
            backend::LeaderValue::Found { id } if id != self.leader_id => {
                anyhow::bail!("leadership has changed")
            }
            backend::LeaderValue::Found { .. } => self
                .backend
                .update(&self.key, &self.leader_id)
                .await
                .context("update leadership lock")?,
        }

        Ok(())
    }

    async fn try_for_leadership(&mut self) -> anyhow::Result<()> {
        self.backend
            .update(&self.key, &self.leader_id)
            .await
            .context("try for leadership")?;

        tokio::time::sleep(DEFAULT_INTERVAL).await;

        tracing::info!("acquired leadership status");

        let leadership_state = self.leadership_status().await?;

        if !leadership_state.is_leader(&self.leader_id) {
            anyhow::bail!("failed to become leader, there is likely some churn going on");
        }

        // We're a leader
        self.state = LeaderState::Leader;

        Ok(())
    }

    async fn leadership_status(&mut self) -> anyhow::Result<LeadershipState> {
        let val = self
            .backend
            .get(&self.key)
            .await
            .inspect_err(|e| tracing::warn!("failed to query for leadership: {}", e))
            .ok();

        Ok(match val {
            Some(backend::LeaderValue::Found { id }) => LeadershipState::Allocated { id },
            Some(backend::LeaderValue::Unknown) => LeadershipState::NotFound,
            None => LeadershipState::NotFound,
        })
    }
}

enum LeadershipState {
    NotFound,
    Allocated { id: LeaderId },
}

impl LeadershipState {
    pub fn is_leader(&self, leader_id: &LeaderId) -> bool {
        match self {
            LeadershipState::Allocated { id } => id == leader_id,
            _ => false,
        }
    }
}
