use std::collections::BTreeSet;
use std::marker::PhantomData;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::Cohort;
use service_engine::error::EngineError;
use service_engine::gate::{ActionName, Affordances};
use service_engine::impact::{Dims, Impact};
use service_engine::name::{NounName, ProjectorName};
use service_engine::persistence::{Aggregate, Persistence, PersistenceExt, PersistenceStyle};
use service_engine::population::{Interest, Population, WindowQuery};
use service_engine::projector::Emission;
use service_engine::view::{Populate, Projector};
use service_engine::visibility::{Cohorts, Visibility};
use service_engine::wire::Noun;
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use super::aggregate::{File, FileRow, PageOrigin, drive_memberships};
use super::store::{FILE_FROM, FileStore, config_error, file_select, row_to_file_prefixed};
use crate::facts::{self, FactMeta, Pending, SoftEda, Stamped};
use crate::host::{DRIVE_DIM, DriveHost};

pub const EDIT_PAGE_ACTION: ActionName = ActionName::from_static("editPage");
pub const REGENERATE_PAGE_ACTION: ActionName = ActionName::from_static("regeneratePage");

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PageKey {
    pub file_id: Uuid,
    pub number: i32,
}

pub struct Page;

impl Noun for Page {
    type Key = PageKey;
    const NAME: NounName = NounName::from_static("drive_page");
}

/// The payload schema version of [`PageEvent`].
pub const PAGE_EVENT_VERSION: i32 = 1;

/// What happened to a page, as the host's fact table records it (the
/// `drive_page` noun, keyed `{file_id, number}`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind")]
#[non_exhaustive]
pub enum PageEvent {
    /// A runner reported the page, in a run of `job_id`.
    Reported { job_id: Uuid, origin: PageOrigin },
    /// A person edited the page.
    Edited,
    /// The chain ended with fewer pages than the file had: the page is gone.
    Trimmed,
}

impl crate::facts::DriveEvent for PageEvent {
    const NOUN: &'static str = "drive_page";
    const VERSION: i32 = PAGE_EVENT_VERSION;

    fn kind(&self) -> &'static str {
        match self {
            Self::Reported { .. } => "Reported",
            Self::Edited => "Edited",
            Self::Trimmed => "Trimmed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum PageCause {
    Reported {
        job_id: Uuid,
        origin: PageOrigin,
    },
    Edited,
    /// The chain ended with fewer pages than the file had: the page is gone.
    Trimmed,
}

pub struct PageRecord<H> {
    pub key: PageKey,
    pub markdown: String,
    pub origin: PageOrigin,
    pub updated_by: Uuid,
    pub updated_at: DateTime<Utc>,
    pub file: FileRow<H>,
    /// One per event of the page (`drive.file_page.version`).
    pub(crate) version: i64,
    pending: Pending<PageEvent>,
}

impl<H> Clone for PageRecord<H> {
    fn clone(&self) -> Self {
        Self {
            key: self.key,
            markdown: self.markdown.clone(),
            origin: self.origin,
            updated_by: self.updated_by,
            updated_at: self.updated_at,
            file: self.file.clone(),
            version: self.version,
            pending: self.pending.clone(),
        }
    }
}

impl<H> PageRecord<H> {
    /// A person rewrote the page: it becomes `EDITED`, theirs, now.
    pub(crate) fn edit(&mut self, markdown: String, meta: &FactMeta) {
        self.markdown = markdown;
        self.origin = PageOrigin::Edited;
        self.updated_by = meta.actor_id;
        self.updated_at = meta.occurred_at;
        self.pending
            .push(&mut self.version, PageEvent::Edited, meta);
    }

    fn base_version(&self) -> i64 {
        self.version - self.pending.len()
    }
}

pub struct PageStore<H>(PhantomData<fn() -> H>);

pub(crate) async fn load_pages<H: DriveHost>(
    conn: &mut PgConnection,
    keys: &[PageKey],
) -> Result<Vec<PageRecord<H>>, EngineError> {
    let files: Vec<Uuid> = keys.iter().map(|key| key.file_id).collect();
    let numbers: Vec<i32> = keys.iter().map(|key| key.number).collect();
    // A page whose last writer is unknown (erased, or carried over without
    // one) reads as the nil id.
    let rows = sqlx::query(&format!(
        "SELECT p.file_id, p.number, p.markdown, p.origin, p.version, \
                COALESCE(p.updated_by, '00000000-0000-0000-0000-000000000000'::uuid) AS updated_by, \
                p.updated_at, {} \
         FROM {FILE_FROM} \
         JOIN drive.file_page p ON p.file_id = f.id \
         JOIN unnest($1::uuid[], $2::int[]) AS wanted(file_id, number) \
           ON wanted.file_id = p.file_id AND wanted.number = p.number \
         ORDER BY p.file_id, p.number",
        file_select("f_"),
    ))
    .bind(&files)
    .bind(&numbers)
    .fetch_all(conn)
    .await?;
    rows.iter()
        .map(|row| {
            let origin: String = row.get("origin");
            Ok(PageRecord {
                key: PageKey {
                    file_id: row.get("file_id"),
                    number: row.get("number"),
                },
                markdown: row.get("markdown"),
                origin: PageOrigin::from_db_str(&origin).map_err(config_error)?,
                updated_by: row.get("updated_by"),
                updated_at: row.get("updated_at"),
                file: row_to_file_prefixed(row, "f_")?,
                version: row.get("version"),
                pending: Pending::default(),
            })
        })
        .collect()
}

impl<H: DriveHost> Persistence for PageStore<H> {
    type Aggregate = PageRecord<H>;
    type Key = PageKey;
    type Event = Stamped<PageEvent>;

    const STYLE: PersistenceStyle = PersistenceStyle::SoftEda;

    fn lock<'a>(
        conn: &'a mut PgConnection,
        key: &'a PageKey,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(
                "SELECT 1 FROM drive.file_page WHERE file_id = $1 AND number = $2 FOR UPDATE",
            )
            .bind(key.file_id)
            .bind(key.number)
            .execute(conn)
            .await?;
            Ok(())
        })
    }

    fn read_many<'a>(
        conn: &'a mut PgConnection,
        keys: &'a [PageKey],
    ) -> BoxFuture<'a, Result<Vec<(PageKey, PageRecord<H>)>, EngineError>> {
        Box::pin(async move {
            Ok(load_pages(conn, keys)
                .await?
                .into_iter()
                .map(|page| (page.key, page))
                .collect())
        })
    }

    fn save<'a>(
        conn: &'a mut PgConnection,
        page: &'a PageRecord<H>,
        events: &'a [Stamped<PageEvent>],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            upsert(conn, page).await?;
            hand_page_facts::<H>(conn, page, events).await
        })
    }

    fn create<'a>(
        conn: &'a mut PgConnection,
        page: &'a PageRecord<H>,
        events: &'a [Stamped<PageEvent>],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            super::processed::ensure(conn, page.key.file_id).await?;
            upsert(conn, page).await?;
            hand_page_facts::<H>(conn, page, events).await
        })
    }
}

async fn upsert<H>(conn: &mut PgConnection, page: &PageRecord<H>) -> Result<(), EngineError> {
    sqlx::query(
        "INSERT INTO drive.file_page \
           (file_id, number, markdown, origin, version, updated_at, updated_by) \
         VALUES ($1, $2, $3, $4, $5, $6, $7) \
         ON CONFLICT (file_id, number) DO UPDATE SET markdown = EXCLUDED.markdown, \
           origin = EXCLUDED.origin, version = EXCLUDED.version, \
           updated_at = EXCLUDED.updated_at, updated_by = EXCLUDED.updated_by",
    )
    .bind(page.key.file_id)
    .bind(page.key.number)
    .bind(&page.markdown)
    .bind(page.origin.as_str())
    .bind(page.version)
    .bind(page.updated_at)
    .bind(page.updated_by)
    .execute(conn)
    .await?;
    Ok(())
}

async fn hand_page_facts<H: DriveHost>(
    conn: &mut PgConnection,
    page: &PageRecord<H>,
    events: &[Stamped<PageEvent>],
) -> Result<(), EngineError> {
    let key = super::store::page_key(page.key.file_id, page.key.number);
    let facts = facts::facts_of(&key, page.base_version(), events)?;
    facts::hand::<H>(conn, &facts).await
}

impl<H: DriveHost> Aggregate for PageRecord<H> {
    type Store = PageStore<H>;

    fn key(&self) -> PageKey {
        self.key
    }

    fn pending_events(&self) -> &[Stamped<PageEvent>] {
        self.pending.as_slice()
    }
}

impl<H: DriveHost> SoftEda for PageRecord<H> {
    fn clear_pending(&mut self) {
        self.pending.clear();
    }
}

pub struct PageVisibility<H>(PhantomData<fn() -> H>);

impl<H: DriveHost> Visibility for PageVisibility<H> {
    type Row = PageRecord<H>;
    type Principal = H;

    const DEPS: service_engine::impact::Deps = H::VISIBILITY_DEPS;

    fn cohorts(row: &PageRecord<H>) -> Cohorts {
        vec![Cohort::uuid(DRIVE_DIM, row.file.drive_id)]
    }

    fn memberships(principal: &H) -> Cohorts {
        drive_memberships(principal)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct DrivePage {
    pub file_id: Uuid,
    pub number: i32,
    pub markdown: String,
    pub origin: PageOrigin,
    pub updated_by: Uuid,
    pub updated_at: DateTime<Utc>,
    pub affordances: Affordances,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PageWindow {
    pub file_id: Option<Uuid>,
}

impl PageWindow {
    pub fn of(file_id: Uuid) -> Self {
        Self {
            file_id: Some(file_id),
        }
    }
}

pub struct DrivePages<H>(PhantomData<fn() -> H>);

impl<H> Default for DrivePages<H> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<H> DrivePages<H> {
    pub const NAME: ProjectorName = ProjectorName::from_static("drive_pages");
}

async fn readable_file<H: DriveHost>(
    conn: &mut PgConnection,
    principal: &H,
    file_id: Uuid,
) -> Result<Option<FileRow<H>>, EngineError> {
    let Some(file) = FileStore::<H>::load(conn, &file_id).await? else {
        return Ok(None);
    };
    let visible = principal.visible_drives().contains(&file.drive_id);
    let allowed = file.read_gate(principal).is_allowed();
    Ok((visible && allowed).then_some(file))
}

impl<H: DriveHost> Projector for DrivePages<H> {
    type Principal = H;
    type Noun = Page;
    type Store = PageStore<H>;
    type Query = PageWindow;
    type Out = DrivePage;
    type Visibility = PageVisibility<H>;

    const NAME: ProjectorName = Self::NAME;

    async fn populate(
        cx: &Populate<'_, H>,
        query: &PageWindow,
    ) -> Result<Population<PageKey>, EngineError> {
        let mut keys = BTreeSet::new();
        if let Some(file_id) = query.file_id {
            let mut conn = cx.pool().acquire().await.map_err(EngineError::from)?;
            if readable_file(&mut conn, cx.principal(), file_id)
                .await?
                .is_some()
            {
                keys = super::store::page_numbers(&mut conn, file_id, cx.limit_all())
                    .await?
                    .into_iter()
                    .map(|number| PageKey { file_id, number })
                    .collect();
            }
        }
        let interest = Interest::new()
            .on_noun(Page::NAME, Dims::EMPTY)
            .on_noun(File::NAME, Dims::EMPTY)
            .on_deps(H::VISIBILITY_DEPS);
        let query =
            WindowQuery::new(interest, Arc::new(|_: &PageKey, _: &Impact| false)).with_keys(keys);
        Ok(Population::Query(query))
    }

    fn project(page: &PageRecord<H>, principal: &H) -> Result<DrivePage, EngineError> {
        Ok(DrivePage {
            file_id: page.key.file_id,
            number: page.key.number,
            markdown: page.markdown.clone(),
            origin: page.origin,
            updated_by: page.updated_by,
            updated_at: page.updated_at,
            affordances: Affordances::from_pairs([
                (EDIT_PAGE_ACTION, page.file.edit_page_gate(principal)),
                (
                    REGENERATE_PAGE_ACTION,
                    page.file.regenerate_page_gate(principal, page.key.number),
                ),
            ]),
        })
    }

    fn emission(_impact: &Impact) -> Emission {
        Emission::PerImpact
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct RunnerPage {
    pub number: i32,
    pub markdown: String,
    pub origin: PageOrigin,
    pub updated_at: DateTime<Utc>,
}

pub async fn read_pages(
    conn: &mut PgConnection,
    file_id: Uuid,
) -> Result<Vec<RunnerPage>, EngineError> {
    let rows = sqlx::query(
        "SELECT p.number, p.markdown, p.origin, p.updated_at \
         FROM drive.file_page p WHERE p.file_id = $1 ORDER BY p.number",
    )
    .bind(file_id)
    .fetch_all(conn)
    .await?;
    rows.iter()
        .map(|row| {
            let origin: String = row.get("origin");
            Ok(RunnerPage {
                number: row.get("number"),
                markdown: row.get("markdown"),
                origin: PageOrigin::from_db_str(&origin).map_err(config_error)?,
                updated_at: row.get("updated_at"),
            })
        })
        .collect()
}
