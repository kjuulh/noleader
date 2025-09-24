use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Context;
use async_nats::jetstream::{self, kv};

use crate::backend::{BackendEdge, Key, LeaderId, LeaderValue};

pub struct NatsBackend {
    bucket: String,
    client: jetstream::Context,

    revision: AtomicU64,
}

impl NatsBackend {
    pub fn new(client: async_nats::Client, bucket: &str) -> Self {
        Self {
            bucket: bucket.into(),
            client: jetstream::new(client),
            revision: AtomicU64::new(0),
        }
    }

    pub async fn create_bucket(&self) -> anyhow::Result<()> {
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
}

#[async_trait::async_trait]
impl BackendEdge for NatsBackend {
    async fn setup(&self) -> anyhow::Result<()> {
        self.create_bucket().await?;

        Ok(())
    }
    async fn get(&self, key: &Key) -> anyhow::Result<LeaderValue> {
        let bucket = self.client.get_key_value(&self.bucket).await?;

        let Some(val) = bucket.get(key).await? else {
            anyhow::bail!("key doesn't exists, we've lost leadership status")
        };

        let Ok(id) = uuid::Uuid::from_slice(&val) else {
            return Ok(LeaderValue::Unknown);
        };

        Ok(LeaderValue::Found { id: id.into() })
    }
    async fn update(&self, key: &Key, val: &LeaderId) -> anyhow::Result<()> {
        let bucket = self
            .client
            .get_key_value(&self.bucket)
            .await
            .context("get bucket")?;

        match bucket
            .update(
                &key.0,
                bytes::Bytes::copy_from_slice(val.as_bytes()),
                self.revision.load(Ordering::Relaxed),
            )
            .await
        {
            Ok(rev) => {
                self.revision.store(rev, Ordering::Relaxed);
            }
            Err(e) => match e.kind() {
                kv::UpdateErrorKind::WrongLastRevision => {
                    tracing::trace!("creating nats entry");
                    match bucket
                        .create_with_ttl(
                            &key.0,
                            bytes::Bytes::copy_from_slice(val.as_bytes()),
                            std::time::Duration::from_secs(60),
                        )
                        .await
                    {
                        Ok(rev) => {
                            self.revision.store(rev, Ordering::Relaxed);
                        }
                        Err(e) => match e.kind() {
                            kv::CreateErrorKind::AlreadyExists => {
                                anyhow::bail!("another candidate has leadership status")
                            }
                            _ => {
                                anyhow::bail!("{}", e);
                            }
                        },
                    }
                }
                _ => {
                    anyhow::bail!("failed to create bucket: {e}")
                }
            },
        }

        Ok(())
    }

    async fn release(&self, _key: &Key, _val: &LeaderId) -> anyhow::Result<()> {
        // TODO: implement release for nats

        Ok(())
    }
}
