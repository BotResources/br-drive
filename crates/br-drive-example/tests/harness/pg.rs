use std::time::Duration;

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

pub const ROLE_PASSWORD: &str = "drive_e2e_only";

pub fn admin_url() -> String {
    std::env::var("E2E_PG_ADMIN_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .expect("E2E_PG_ADMIN_URL or DATABASE_URL must point at a real PostgreSQL superuser")
}

fn url_for(admin: &str, role: &str, database: &str) -> String {
    let base = admin
        .split('@')
        .next_back()
        .unwrap_or("localhost:5432/postgres");
    let host_port = base.split('/').next().unwrap_or("localhost:5432");
    format!("postgresql://{role}:{ROLE_PASSWORD}@{host_port}/{database}")
}

pub struct TestDb {
    pub admin: PgPool,
    pub app: PgPool,
    pub database: String,
    pub owner_role: String,
    pub app_role: String,
}

impl TestDb {
    pub async fn fresh() -> Self {
        let admin_url = admin_url();
        let admin = pool(&admin_url).await;

        let suffix = Uuid::now_v7().simple().to_string();
        let database = format!("drv_{suffix}_db");
        let owner_role = format!("drv_{suffix}_owner");
        let app_role = format!("drv_{suffix}_app");

        run(
            &admin,
            &format!(
                "CREATE ROLE \"{owner_role}\" LOGIN CREATEROLE NOSUPERUSER BYPASSRLS PASSWORD '{ROLE_PASSWORD}'"
            ),
        )
        .await;
        run(
            &admin,
            &format!("CREATE DATABASE \"{database}\" OWNER \"{owner_role}\""),
        )
        .await;

        let owner = pool(&url_for(&admin_url, &owner_role, &database)).await;
        ensure_app_role(&owner, &app_role, ROLE_PASSWORD).await;
        run(
            &admin,
            &format!("GRANT CONNECT ON DATABASE \"{database}\" TO \"{app_role}\""),
        )
        .await;

        service_engine::engine::boot::apply_migration_chain(
            &owner,
            br_drive_example::db::libraries(),
            br_drive_example::db::migrator(),
            &app_role,
            Duration::from_secs(10),
        )
        .await
        .expect("apply the engine, library and service migration sets");

        let app = pool(&url_for(&admin_url, &app_role, &database)).await;
        owner.close().await;

        Self {
            admin,
            app,
            database,
            owner_role,
            app_role,
        }
    }

    pub async fn cleanup(self) {
        let admin = self.admin.clone();
        self.app.close().await;
        let _ = sqlx::query(&format!(
            "DROP DATABASE IF EXISTS \"{}\" WITH (FORCE)",
            self.database
        ))
        .execute(&admin)
        .await;
        for role in [self.app_role, self.owner_role] {
            let _ = sqlx::query(&format!("DROP ROLE IF EXISTS \"{role}\""))
                .execute(&admin)
                .await;
        }
        admin.close().await;
    }
}

async fn ensure_app_role(owner: &PgPool, role: &str, password: &str) {
    run(
        owner,
        &format!(
            "DO $$ BEGIN \
               IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}') THEN \
                 CREATE ROLE \"{role}\" LOGIN; \
               END IF; \
             END $$"
        ),
    )
    .await;
    run(
        owner,
        &format!("ALTER ROLE \"{role}\" LOGIN PASSWORD '{password}'"),
    )
    .await;
}

async fn pool(url: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(6)
        .connect(url)
        .await
        .unwrap_or_else(|e| panic!("connect to postgres: {e}"))
}

async fn run(pool: &PgPool, sql: &str) {
    sqlx::query(sql)
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}
