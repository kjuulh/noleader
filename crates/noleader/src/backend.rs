use std::{ops::Deref, sync::Arc};


    #[cfg(feature = "nats")]
mod nats;
    #[cfg(feature = "postgres")]
mod postgres;

pub struct Backend {
    inner: Arc<dyn BackendEdge + Send + Sync + 'static>,
}

impl Backend {
    pub fn new(edge: impl BackendEdge + Send + Sync + 'static) -> Self {
        Self {
            inner: Arc::new(edge),
        }
    }

    #[cfg(feature = "nats")]
    pub fn nats(client: async_nats::Client, bucket: &str) -> Self {
        Self {
            inner: Arc::new(nats::NatsBackend::new(client, bucket)),
        }
    }

    #[cfg(feature = "postgres")]
    pub fn postgres(database_url: &str) -> Self {
        Self {
            inner: Arc::new(postgres::PostgresBackend::new(database_url)),
        }
    }

    #[cfg(feature = "postgres")]
    pub fn postgres_with_pool(pool: sqlx::PgPool) -> Self {
        Self {
            inner: Arc::new(postgres::PostgresBackend::new_with_pool("bogus", pool)),
        }
    }
}

impl Deref for Backend {
    type Target = Arc<dyn BackendEdge + Send + Sync + 'static>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[async_trait::async_trait]
pub trait BackendEdge {
    async fn setup(&self) -> anyhow::Result<()>;
    async fn get(&self, key: &Key) -> anyhow::Result<LeaderValue>;
    async fn update(&self, key: &Key, val: &LeaderId) -> anyhow::Result<()>;
    async fn release(&self, key: &Key, val: &LeaderId) -> anyhow::Result<()>;
}

pub enum LeaderValue {
    Unknown,
    Found { id: LeaderId },
}

pub struct Key(String);

impl From<String> for Key {
    fn from(value: String) -> Self {
        Self(value)
    }
}
impl From<&str> for Key {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<Key> for String {
    fn from(value: Key) -> Self {
        value.0
    }
}
impl From<&Key> for String {
    fn from(value: &Key) -> Self {
        value.0.clone()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaderId(uuid::Uuid);
impl LeaderId {
    pub(crate) fn new() -> Self {
        Self(uuid::Uuid::now_v7())
    }
}

impl From<LeaderId> for uuid::Uuid {
    fn from(value: LeaderId) -> Self {
        value.0
    }
}
impl From<&LeaderId> for uuid::Uuid {
    fn from(value: &LeaderId) -> Self {
        value.0
    }
}

impl From<uuid::Uuid> for LeaderId {
    fn from(value: uuid::Uuid) -> Self {
        Self(value)
    }
}

impl LeaderId {
    pub const fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}
