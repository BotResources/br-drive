//! A file's results (`drive.file_processed`): what the workers produced — the
//! indexer's triple here, the pages and the extracted images hanging off the
//! row by their foreign keys. The row holds no state: it is created by the
//! first write that needs it and goes with its file.

use sqlx::{PgConnection, Row};
use uuid::Uuid;

use service_engine::error::EngineError;

use super::images::references_image;

/// Creates the file's results row if it has none yet.
pub(crate) async fn ensure(conn: &mut PgConnection, file_id: Uuid) -> Result<(), EngineError> {
    sqlx::query(
        "INSERT INTO drive.file_processed (file_id) VALUES ($1) ON CONFLICT (file_id) DO NOTHING",
    )
    .bind(file_id)
    .execute(conn)
    .await?;
    Ok(())
}

/// Records an indexing: the three fields describe one indexing, never two.
pub(crate) async fn store_indexing(
    conn: &mut PgConnection,
    file_id: Uuid,
    summary: Option<&str>,
    page_count: Option<i32>,
    estimated_tokens: Option<i64>,
) -> Result<(), EngineError> {
    sqlx::query(
        "INSERT INTO drive.file_processed (file_id, summary, page_count, estimated_tokens) \
         VALUES ($1, $2, $3, $4) \
         ON CONFLICT (file_id) DO UPDATE SET summary = EXCLUDED.summary, \
           page_count = EXCLUDED.page_count, estimated_tokens = EXCLUDED.estimated_tokens",
    )
    .bind(file_id)
    .bind(summary)
    .bind(page_count)
    .bind(estimated_tokens)
    .execute(conn)
    .await?;
    Ok(())
}

/// The pages among `numbers` a person edited.
pub(crate) async fn edited_among(
    conn: &mut PgConnection,
    file_id: Uuid,
    numbers: &[i32],
) -> Result<Vec<i32>, EngineError> {
    if numbers.is_empty() {
        return Ok(Vec::new());
    }
    let rows = sqlx::query(
        "SELECT number FROM drive.file_page \
         WHERE file_id = $1 AND number = ANY($2) AND origin = 'edited'",
    )
    .bind(file_id)
    .bind(numbers)
    .fetch_all(conn)
    .await?;
    Ok(rows.iter().map(|row| row.get::<i32, _>("number")).collect())
}

/// The file's images no page's markdown references any more (by whole name,
/// as `references_image` matches it), in name order.
pub(crate) async fn unreferenced_images(
    conn: &mut PgConnection,
    file_id: Uuid,
) -> Result<Vec<String>, EngineError> {
    let names = super::images::image_names_of(&mut *conn, file_id).await?;
    if names.is_empty() {
        return Ok(Vec::new());
    }
    let rows = sqlx::query("SELECT markdown FROM drive.file_page WHERE file_id = $1")
        .bind(file_id)
        .fetch_all(conn)
        .await?;
    let pages: Vec<String> = rows
        .iter()
        .map(|row| row.get::<String, _>("markdown"))
        .collect();
    Ok(names
        .into_iter()
        .filter(|name| {
            !pages
                .iter()
                .any(|markdown| references_image(markdown, name))
        })
        .collect())
}
