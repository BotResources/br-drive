//! The engine's erase pipeline over the library's rows: what happens to what a
//! person created when the host erases that person.

use std::marker::PhantomData;

use futures_util::future::BoxFuture;
use service_engine::BlobRef;
use service_engine::erase::{Erasable, Erase, Erased, PersonId};
use service_engine::error::EngineError;
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::file::store;
use crate::file::{File, FileCause};
use crate::host::DriveHost;

/// The person id every anonymised `created_by` / `updated_by` / `triggered_by`
/// is rewritten to: the nil UUID, which no real principal carries.
pub const REDACTED_PERSON: Uuid = Uuid::nil();

/// What the library does with a person's rows when the host erases them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EraseMode {
    /// Rewrite the person's ids on drives, files, pages, images, labels and
    /// label links to [`REDACTED_PERSON`]; nothing is deleted.
    Anonymise,
    /// Delete every file the person created (pages, images and links cascade,
    /// the objects are purged) and anonymise the rest. Drives are the host's:
    /// it deletes them with `delete_drive`.
    Delete,
}

pub struct DriveErasure<H>(PhantomData<fn() -> H>);

impl<H> Default for DriveErasure<H> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

async fn anonymise(conn: &mut PgConnection, person: Uuid) -> Result<u64, EngineError> {
    let mut rows = 0;
    for (table, column) in [
        ("drive.drive", "created_by"),
        ("drive.file", "created_by"),
        ("drive.file_page", "updated_by"),
        ("drive.label", "created_by"),
        ("drive.file_label", "created_by"),
        ("drive.ruleset", "created_by"),
    ] {
        let done = sqlx::query(&format!(
            "UPDATE {table} SET {column} = $2 WHERE {column} = $1"
        ))
        .bind(person)
        .bind(REDACTED_PERSON)
        .execute(&mut *conn)
        .await?;
        rows += done.rows_affected();
    }
    let done = sqlx::query(
        "UPDATE drive.file_job SET triggered_by = jsonb_set(triggered_by, '{id}', to_jsonb($2::text)) \
         WHERE triggered_by ->> 'id' = $1::text",
    )
    .bind(person)
    .bind(REDACTED_PERSON)
    .execute(&mut *conn)
    .await?;
    rows += done.rows_affected();
    let done = sqlx::query(
        "UPDATE drive.file_job SET triggered_by = triggered_by - 'display_name' || '{\"display_name\": null}'::jsonb \
         WHERE triggered_by ->> 'id' = $1::text",
    )
    .bind(REDACTED_PERSON)
    .execute(conn)
    .await?;
    rows += done.rows_affected();
    Ok(rows)
}

async fn files_created_by(conn: &mut PgConnection, person: Uuid) -> Result<Vec<Uuid>, EngineError> {
    let rows = sqlx::query("SELECT id FROM drive.file WHERE created_by = $1")
        .bind(person)
        .fetch_all(conn)
        .await?;
    Ok(rows.iter().map(|row| row.get::<Uuid, _>("id")).collect())
}

async fn source_refs(conn: &mut PgConnection, files: &[Uuid]) -> Result<Vec<Uuid>, EngineError> {
    let rows = sqlx::query("SELECT blob_ref FROM drive.file WHERE id = ANY($1)")
        .bind(files)
        .fetch_all(conn)
        .await?;
    Ok(rows
        .iter()
        .map(|row| row.get::<Uuid, _>("blob_ref"))
        .collect())
}

impl<H: DriveHost> Erasable for DriveErasure<H> {
    type Error = EngineError;

    fn erase<'a>(
        &'a self,
        cx: &'a mut Erase<'a>,
        person: PersonId,
    ) -> BoxFuture<'a, Result<Erased, Self::Error>> {
        Box::pin(async move {
            let mut erased = Erased::new();
            let person = person.as_uuid();
            if H::erase_mode() == EraseMode::Delete {
                let files = files_created_by(cx.connection(), person).await?;
                if !files.is_empty() {
                    // The erase pipeline runs without the blob handle, so the
                    // objects go through the manifest's purge, not a release.
                    let sources = source_refs(cx.connection(), &files).await?;
                    let drives = if crate::owner::refreshes::<H>() {
                        store::drives_of(cx.connection(), &files).await?
                    } else {
                        Vec::new()
                    };
                    let images = store::image_refs_of_files(cx.connection(), &files).await?;
                    // The erase pipeline has no outbound identity either, so a
                    // running job cannot be cancelled from here: the deleted
                    // file refuses the runner's next call, and the job is
                    // left to Jobs' own backstops (none fires for a job no
                    // runner ever picked up; an administrator cancels it in
                    // Jobs, the file being gone).
                    let deleted = store::delete_many(cx.connection(), &files).await?;
                    erased.rows(deleted);
                    for reference in sources.into_iter().chain(images) {
                        erased.purge_blob(BlobRef(reference));
                    }
                    // The erase context is a plain `Ops`: it cannot stage a
                    // projector reset the way the bulk pipeline does, so the
                    // live sessions are told file by file up to the host's bulk
                    // threshold and catch up on their next reset past it.
                    for file in files.iter().take(H::BULK_RESET_THRESHOLD) {
                        cx.impact_caused::<File, _>(file, FileCause::Erased)?;
                    }
                    for drive in drives {
                        crate::owner::touch::<H>(cx, drive)?;
                    }
                }
            }
            let rows = anonymise(cx.connection(), person).await?;
            erased.rows(rows);
            Ok(erased)
        })
    }
}
