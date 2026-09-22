use sqlx::migrate::Migrator;

pub fn migrator() -> Migrator {
    sqlx::migrate!("./migrations")
}

pub fn libraries() -> Vec<service_engine::LibraryMigrations> {
    #[cfg(feature = "drive")]
    {
        vec![br_drive::migrations()]
    }
    #[cfg(not(feature = "drive"))]
    {
        Vec::new()
    }
}
