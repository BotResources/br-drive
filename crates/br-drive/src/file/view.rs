use std::collections::BTreeSet;
use std::marker::PhantomData;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use service_engine::JsonScalar;
use service_engine::error::EngineError;
use service_engine::gate::{Affordances, Gated};
use service_engine::impact::Impact;
use service_engine::name::ProjectorName;
use service_engine::population::Population;
use service_engine::projector::Emission;
use service_engine::view::{Populate, Projector, windowed};
use uuid::Uuid;

use super::aggregate::{
    File, FileRow, FileVisibility, ImageRow, PageOrigin, PageRow, ProcessingState,
};
use super::store::{self, FileStore};
use crate::host::DriveHost;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ByteCount(pub u64);

async_graphql::scalar!(ByteCount);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct DrivePage {
    pub number: i32,
    pub markdown: String,
    pub origin: PageOrigin,
    pub updated_by: Uuid,
    pub updated_at: DateTime<Utc>,
}

impl From<&PageRow> for DrivePage {
    fn from(page: &PageRow) -> Self {
        Self {
            number: page.number,
            markdown: page.markdown.clone(),
            origin: page.origin,
            updated_by: page.updated_by,
            updated_at: page.updated_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct DriveImage {
    pub name: String,
    pub media_type: String,
    pub size_bytes: ByteCount,
    pub page: Option<i32>,
    #[graphql(skip)]
    pub source: Uuid,
}

impl From<&ImageRow> for DriveImage {
    fn from(image: &ImageRow) -> Self {
        Self {
            name: image.name.as_str().to_string(),
            media_type: image.media_type.as_str().to_string(),
            size_bytes: ByteCount(u64::try_from(image.size_bytes).unwrap_or(0)),
            page: image.page,
            source: image.blob_ref,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct DriveFile {
    pub id: Uuid,
    pub drive_id: Uuid,
    pub path: String,
    pub name: String,
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
    pub pages: Vec<DrivePage>,
    pub images: Vec<DriveImage>,
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

impl<H: DriveHost> Projector for DriveFiles<H> {
    type Principal = H;
    type Noun = File;
    type Store = FileStore<H>;
    type Query = DriveWindow;
    type Out = DriveFile;
    type Visibility = FileVisibility<H>;

    const NAME: ProjectorName = Self::NAME;

    async fn populate(
        cx: &Populate<'_, H>,
        query: &DriveWindow,
    ) -> Result<Population<Uuid>, EngineError> {
        let visible = cx.principal().visible_drives();
        let drives: Vec<Uuid> = match query.drive_id {
            Some(drive) if visible.contains(&drive) => vec![drive],
            Some(_) => Vec::new(),
            None => visible,
        };
        let keys: BTreeSet<Uuid> = if drives.is_empty() {
            BTreeSet::new()
        } else {
            let mut conn = cx.pool().acquire().await.map_err(EngineError::from)?;
            store::ids_in_drives(&mut conn, &drives)
                .await?
                .into_iter()
                .collect()
        };
        Ok(windowed::<Self>(keys))
    }

    fn project(row: &FileRow<H>, principal: &H) -> Result<DriveFile, EngineError> {
        Ok(DriveFile {
            id: row.id,
            drive_id: row.drive_id,
            path: row.path.as_str().to_string(),
            name: row.name.as_str().to_string(),
            protected: row.protected,
            media_type: row.media_type.as_str().to_string(),
            size_bytes: ByteCount(u64::try_from(row.size_bytes).unwrap_or(0)),
            sha256: hex(&row.sha256),
            processing_state: row.processing_state,
            processing_error: row.processing_error.clone(),
            metadata: async_graphql::Json(row.metadata.clone()),
            summary: row.summary.clone(),
            page_count: row.page_count,
            estimated_tokens: row.estimated_tokens,
            pages: row.pages.iter().map(DrivePage::from).collect(),
            images: row.images.iter().map(DriveImage::from).collect(),
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
