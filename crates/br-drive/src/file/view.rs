use std::collections::BTreeSet;
use std::marker::PhantomData;

use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::JsonScalar;
use service_engine::error::EngineError;
use service_engine::gate::{Affordances, Gated};
use service_engine::impact::Impact;
use service_engine::name::ProjectorName;
use service_engine::persistence::{Aggregate, Persistence, PersistenceStyle};
use service_engine::population::Population;
use service_engine::projector::Emission;
use service_engine::view::{Populate, Projector, windowed};
use service_engine::visibility::{Cohorts, Visibility};
use service_engine::{BlobRef, Cohort};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use super::aggregate::{File, FileRow, ProcessingState, drive_memberships};
use super::store::{self, FILE_FROM, file_select, row_to_file};
use crate::host::{DRIVE_DIM, DriveHost};
use crate::ruleset::DriveStep;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ByteCount(pub u64);

async_graphql::scalar!(ByteCount);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageSummary {
    pub name: String,
    pub media_type: String,
    pub size_bytes: i64,
    pub page: i32,
    pub blob_ref: Uuid,
}

pub struct FileView<H> {
    pub file: FileRow<H>,
    pub images: Vec<ImageSummary>,
    pub label_ids: Vec<Uuid>,
}

impl<H> Clone for FileView<H> {
    fn clone(&self) -> Self {
        Self {
            file: self.file.clone(),
            images: self.images.clone(),
            label_ids: self.label_ids.clone(),
        }
    }
}

pub struct FileViewStore<H>(PhantomData<fn() -> H>);

async fn load_views<H: DriveHost>(
    conn: &mut PgConnection,
    keys: &[Uuid],
) -> Result<Vec<FileView<H>>, EngineError> {
    let rows = sqlx::query(&format!(
        "SELECT {} FROM {FILE_FROM} WHERE f.id = ANY($1)",
        file_select("")
    ))
    .bind(keys)
    .fetch_all(&mut *conn)
    .await?;
    let mut views: Vec<FileView<H>> = rows
        .iter()
        .map(|row| {
            row_to_file(row).map(|file| FileView {
                file,
                images: Vec::new(),
                label_ids: Vec::new(),
            })
        })
        .collect::<Result<_, _>>()?;
    if views.is_empty() {
        return Ok(views);
    }
    let images = sqlx::query(
        "SELECT file_id, name, media_type, size_bytes, page, blob_ref \
         FROM drive.file_image WHERE file_id = ANY($1) ORDER BY file_id, name",
    )
    .bind(keys)
    .fetch_all(&mut *conn)
    .await?;
    let mut labels = crate::label::label_ids_of_files(&mut *conn, keys).await?;
    for view in &mut views {
        if let Some(ids) = labels.remove(&view.file.id) {
            view.label_ids = ids;
        }
    }
    for row in &images {
        let file_id: Uuid = row.get("file_id");
        if let Some(view) = views.iter_mut().find(|view| view.file.id == file_id) {
            view.images.push(ImageSummary {
                name: row.get("name"),
                media_type: row.get("media_type"),
                size_bytes: row.get("size_bytes"),
                page: row.get("page"),
                blob_ref: row.get("blob_ref"),
            });
        }
    }
    Ok(views)
}

fn read_only() -> EngineError {
    EngineError::Config("the file view store is read-only; writes go through FileStore".into())
}

impl<H: DriveHost> Persistence for FileViewStore<H> {
    type Aggregate = FileView<H>;
    type Key = Uuid;
    type Event = ();

    const STYLE: PersistenceStyle = PersistenceStyle::Crud;

    fn load<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<Option<FileView<H>>, EngineError>> {
        Box::pin(async move { Ok(load_views(conn, std::slice::from_ref(key)).await?.pop()) })
    }

    fn read_many<'a>(
        conn: &'a mut PgConnection,
        keys: &'a [Uuid],
    ) -> BoxFuture<'a, Result<Vec<(Uuid, FileView<H>)>, EngineError>> {
        Box::pin(async move {
            Ok(load_views(conn, keys)
                .await?
                .into_iter()
                .map(|view| (view.file.id, view))
                .collect())
        })
    }

    fn save<'a>(
        _conn: &'a mut PgConnection,
        _view: &'a FileView<H>,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async { Err(read_only()) })
    }

    fn create<'a>(
        _conn: &'a mut PgConnection,
        _view: &'a FileView<H>,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async { Err(read_only()) })
    }
}

impl<H: DriveHost> Aggregate for FileView<H> {
    type Store = FileViewStore<H>;

    fn key(&self) -> Uuid {
        self.file.id
    }

    fn blob_refs(&self) -> Vec<BlobRef> {
        std::iter::once(BlobRef(self.file.blob_ref))
            .chain(self.images.iter().map(|image| BlobRef(image.blob_ref)))
            .collect()
    }
}

pub struct FileViewVisibility<H>(PhantomData<fn() -> H>);

impl<H: DriveHost> Visibility for FileViewVisibility<H> {
    type Row = FileView<H>;
    type Principal = H;

    const DEPS: service_engine::impact::Deps = H::VISIBILITY_DEPS;

    fn cohorts(row: &FileView<H>) -> Cohorts {
        vec![Cohort::uuid(DRIVE_DIM, row.file.drive_id)]
    }

    fn memberships(principal: &H) -> Cohorts {
        drive_memberships(principal)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct DriveImage {
    pub name: String,
    pub media_type: String,
    pub size_bytes: ByteCount,
    pub page: i32,
    #[graphql(skip)]
    pub source: Uuid,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct DriveProgress {
    pub step_index: i32,
    pub step_count: i32,
    pub runner_type: String,
    pub plan: Vec<String>,
    pub current_index: Option<i32>,
    pub current_label: Option<String>,
    pub at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct DriveFile {
    pub id: Uuid,
    pub drive_id: Uuid,
    pub path: String,
    pub name: String,
    pub title: String,
    pub protected: bool,
    pub media_type: String,
    pub size_bytes: ByteCount,
    pub sha256: String,
    pub processing_state: ProcessingState,
    pub processing_error: Option<String>,
    pub metadata: JsonScalar,
    pub summary: Option<String>,
    pub page_count: Option<i32>,
    pub estimated_tokens: Option<i64>,
    pub images: Vec<DriveImage>,
    pub label_ids: Vec<Uuid>,
    pub ruleset_id: Option<Uuid>,
    pub steps: Option<Vec<DriveStep>>,
    pub progress: Option<DriveProgress>,
    pub created_by: Uuid,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub affordances: Affordances,
    #[graphql(skip)]
    pub source: Uuid,
}

impl DriveFile {
    pub fn is_ready(&self) -> bool {
        self.processing_state == ProcessingState::Ready
    }

    pub fn image(&self, name: &str) -> Option<&DriveImage> {
        self.images.iter().find(|image| image.name == name)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DriveWindow {
    pub drive_id: Option<Uuid>,
}

impl DriveWindow {
    pub fn of(drive_id: Uuid) -> Self {
        Self {
            drive_id: Some(drive_id),
        }
    }
}

pub struct DriveFiles<H>(PhantomData<fn() -> H>);

impl<H> Default for DriveFiles<H> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<H> DriveFiles<H> {
    pub const NAME: ProjectorName = ProjectorName::from_static("drive_files");
}

pub(crate) fn hex(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub(crate) async fn visible_file_keys<H: DriveHost>(
    cx: &Populate<'_, H>,
    drive_id: Option<Uuid>,
) -> Result<BTreeSet<Uuid>, EngineError> {
    let visible = cx.principal().visible_drives();
    let drives: Vec<Uuid> = match drive_id {
        Some(drive) if visible.contains(&drive) => vec![drive],
        Some(_) => Vec::new(),
        None => visible,
    };
    if drives.is_empty() {
        return Ok(BTreeSet::new());
    }
    let mut conn = cx.pool().acquire().await.map_err(EngineError::from)?;
    Ok(store::ids_in_drives(&mut conn, &drives)
        .await?
        .into_iter()
        .collect())
}

/// Where the running chain is, while the file is PROCESSING: the step of
/// the file's active job, and what that job's runner said of its run.
fn progress_of<H>(row: &FileRow<H>) -> Option<DriveProgress> {
    let job = row.active_job()?;
    let steps = row.steps.as_deref().unwrap_or_default();
    let run = job.progress();
    Some(DriveProgress {
        step_index: job.step_index,
        step_count: i32::try_from(steps.len()).unwrap_or(i32::MAX),
        runner_type: usize::try_from(job.step_index)
            .ok()
            .and_then(|index| steps.get(index))
            .map(|step| step.runner_type.clone())
            .unwrap_or_default(),
        plan: run.plan,
        current_index: run.current_index,
        current_label: run.current_label,
        at: run.at,
    })
}

impl<H: DriveHost> Projector for DriveFiles<H> {
    type Principal = H;
    type Noun = File;
    type Store = FileViewStore<H>;
    type Query = DriveWindow;
    type Out = DriveFile;
    type Visibility = FileViewVisibility<H>;

    const NAME: ProjectorName = Self::NAME;

    async fn populate(
        cx: &Populate<'_, H>,
        query: &DriveWindow,
    ) -> Result<Population<Uuid>, EngineError> {
        Ok(windowed::<Self>(
            visible_file_keys(cx, query.drive_id).await?,
        ))
    }

    fn project(view: &FileView<H>, principal: &H) -> Result<DriveFile, EngineError> {
        let row = &view.file;
        Ok(DriveFile {
            id: row.id,
            drive_id: row.drive_id,
            path: row.path.as_str().to_string(),
            name: row.name.as_str().to_string(),
            title: row.title.as_str().to_string(),
            protected: row.protected,
            media_type: row.media_type.as_str().to_string(),
            size_bytes: ByteCount(u64::try_from(row.size_bytes).unwrap_or(0)),
            sha256: hex(&row.sha256),
            processing_state: row.processing_state(),
            processing_error: row.processing_error().map(str::to_string),
            metadata: async_graphql::Json(row.metadata.clone()),
            summary: row.summary.clone(),
            page_count: row.page_count,
            estimated_tokens: row.estimated_tokens,
            images: view
                .images
                .iter()
                .map(|image| DriveImage {
                    name: image.name.clone(),
                    media_type: image.media_type.clone(),
                    size_bytes: ByteCount(u64::try_from(image.size_bytes).unwrap_or(0)),
                    page: image.page,
                    source: image.blob_ref,
                })
                .collect(),
            label_ids: view.label_ids.clone(),
            ruleset_id: row.ruleset_id,
            steps: row
                .steps
                .as_ref()
                .map(|steps| steps.iter().map(DriveStep::from).collect()),
            progress: progress_of(row),
            created_by: row.created_by,
            created_at: row.created_at,
            updated_at: row.updated_at,
            affordances: row.affordances(principal),
            source: row.blob_ref,
        })
    }

    fn emission(_impact: &Impact) -> Emission {
        Emission::PerImpact
    }
}
