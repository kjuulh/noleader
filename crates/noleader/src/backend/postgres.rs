use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use anyhow::Context;
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::sync::OnceCell;

use crate::backend::{BackendEdge, Key, LeaderId, LeaderValue};

pub struct PostgresBackend {
    database_url: String,
    revision: AtomicU64,
    pool: OnceCell<PgPool>,
    migrated: OnceCell<()>,
}

impl PostgresBackend {
    pub fn new(database_url: &str) -> Self {
        Self {
            database_url: database_url.into(),
            revision: AtomicU64::new(0),
            pool: OnceCell::new(),
            migrated: OnceCell::new(),
        }
    }

    pub fn new_with_pool(database_url: &str, pool: PgPool) -> Self {
        Self {
            database_url: database_url.into(),
            revision: AtomicU64::new(0),
            pool: OnceCell::new_with(Some(pool)),
            migrated: OnceCell::new(),
        }
    }

    async fn db(&self) -> anyhow::Result<PgPool> {
        let pool = self
            .pool
            .get_or_try_init(|| async move {
                PgPoolOptions::new()
                    .max_connections(1)
                    .min_connections(0)
                    .idle_timeout(Some(Duration::from_secs(5)))
                    .connect_lazy(&self.database_url)
                    .context("connect postgres noleader")
            })
            .await?;

        Ok(pool.clone())
    }

    async fn migrate(&self) -> anyhow::Result<()> {
        self.migrated
            .get_or_try_init(|| async move {
                let db = self.db().await?;

                let mut migrate = sqlx::migrate!("./migrations/postgres/");

                migrate
                    .set_locking(false)
                    .dangerous_set_table_name("_sqlx_noleader_migrations")
                    .run(&db)
                    .await
                    .context("migrate noleader")?;

                Ok::<_, anyhow::Error>(())
            })
            .await?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl BackendEdge for PostgresBackend {
    async fn setup(&self) -> anyhow::Result<()> {
        self.migrate().await?;
        Ok(())
    }

    async fn get(&self, key: &Key) -> anyhow::Result<LeaderValue> {
        let rec: Option<GetResult> = sqlx::query_as(
            "
            SELECT value, revision
            FROM noleader_leaders
            WHERE
                  key = $1
              AND heartbeat >= now() - interval '60 seconds'
            LIMIT 1;
            ",
        )
        .bind(&key.0)
        .fetch_optional(&self.db().await?)
        .await
        .context("get noleader key")?;

        let Some(val) = rec else {
            anyhow::bail!("key doesn't exist, we've lost leadership status")
        };

        // Update our local revision to match what's in the database
        self.revision.store(val.revision as u64, Ordering::Relaxed);

        let Ok(id) = uuid::Uuid::parse_str(&val.value) else {
            tracing::warn!("value is not a valid uuid: {}", val.value);
            return Ok(LeaderValue::Unknown);
        };

        Ok(LeaderValue::Found { id: id.into() })
    }

    async fn update(&self, key: &Key, val: &LeaderId) -> anyhow::Result<()> {
        let current_rev = self.revision.load(Ordering::Relaxed);
        let new_rev = current_rev + 1;

        let res: Result<Option<UpdateResult>, sqlx::Error> = sqlx::query_as(
            r#"
            INSERT INTO noleader_leaders (key, value, revision, heartbeat)
            VALUES ($1, $2, $3, now())
            ON CONFLICT (key)
            DO UPDATE SET
                value = EXCLUDED.value,
                revision = EXCLUDED.revision,
                heartbeat = now()
            WHERE 
                (
                    -- Normal case: revision matches (we're the current leader updating)
                    noleader_leaders.revision = $4
                    OR
                    -- Override case: heartbeat is old (stale leader)
                    noleader_leaders.heartbeat < now() - INTERVAL '60 seconds'
                )
            RETURNING value, revision
            "#,
        )
        .bind(&key.0)
        .bind(val.0.to_string())
        .bind(new_rev as i64) // new revision
        .bind(current_rev as i64) // expected current revision
        .fetch_optional(&self.db().await?)
        .await;

        let res = match res {
            Ok(res) => res,
            Err(e) => match &e {
                sqlx::Error::Database(database_error) => {
                    if database_error.is_unique_violation() {
                        anyhow::bail!("update conflict: another leader holds lock")
                    } else {
                        anyhow::bail!(e);
                    }
                }
                _ => {
                    anyhow::bail!(e);
                }
            },
        };

        match res {
            Some(rec) => {
                if rec.value == val.0.to_string() && rec.revision == new_rev as i64 {
                    tracing::debug!(
                        val = val.0.to_string(),
                        revision = rec.revision,
                        "successfully updated leader"
                    );

                    // Only update our local revision if the update succeeded with our expected value
                    self.revision.store(rec.revision as u64, Ordering::Relaxed);
                } else {
                    anyhow::bail!(
                        "update conflict: expected value={}, revision={}, got value={}, revision={}",
                        val.0.to_string(),
                        new_rev,
                        rec.value,
                        rec.revision
                    );
                }
            }
            None => {
                anyhow::bail!(
                    "update rejected: another leader is holding the lock or revision mismatch"
                )
            }
        }

        Ok(())
    }

    async fn release(&self, key: &Key, val: &LeaderId) -> anyhow::Result<()> {
        let rev = self.revision.load(Ordering::Relaxed);
        sqlx::query(
            "
                DELETE FROM noleader_leaders
                WHERE
                        key = $1
                    AND value = $2
                    AND revision = $3
            ",
        )
        .bind(&key.0)
        .bind(val.0.to_string())
        .bind(rev as i64) // new revision
        .execute(&self.db().await?)
        .await
        .context("failed to release lock, it will expire naturally")?;

        Ok(())
    }
}

#[derive(sqlx::FromRow)]
struct GetResult {
    value: String,
    revision: i64,
}

#[derive(sqlx::FromRow)]
struct UpdateResult {
    value: String,
    revision: i64,
}
