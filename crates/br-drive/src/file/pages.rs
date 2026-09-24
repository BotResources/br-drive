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
use service_engine::persistence::{Persistence, PersistenceStyle};
use service_engine::population::{Interest, Population, WindowQuery};
use service_engine::projector::Emission;
use service_engine::view::{Populate, Projector};
use service_engine::visibility::{Cohorts, Visibility};
use service_engine::wire::Noun;
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use super::aggregate::{File, FileRow, PageOrigin, drive_memberships};
use super::store::{FileStore, config_error, file_select, row_to_file_prefixed};
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum PageCause {
    Reported { job_id: Uuid, origin: PageOrigin },
    Edited,
    Imported { origin: PageOrigin },
}

pub struct PageRecord<H> {
    pub key: PageKey,
    pub markdown: String,
    pub origin: PageOrigin,
    pub updated_by: Uuid,
    pub updated_at: DateTime<Utc>,
    pub file: FileRow<H>,
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
        }
    }
}

pub struct PageStore<H>(PhantomData<fn() -> H>);

fn read_only() -> EngineError {
    EngineError::Config("pages are written through the file's report and edit gestures".into())
}

pub(crate) async fn load_pages<H: DriveHost>(
    conn: &mut PgConnection,
    keys: &[PageKey],
) -> Result<Vec<PageRecord<H>>, EngineError> {
    let files: Vec<Uuid> = keys.iter().map(|key| key.file_id).collect();
    let numbers: Vec<i32> = keys.iter().map(|key| key.number).collect();
    let rows = sqlx::query(&format!(
        "SELECT p.file_id, p.number, p.markdown, p.origin, p.updated_by, p.updated_at, {} \
         FROM drive.file_page p \
         JOIN drive.file f ON f.id = p.file_id \
         JOIN drive.file_status s ON s.file_id = f.id \
         JOIN unnest($1::uuid[], $2::int[]) AS wanted(file_id, number) \
           ON wanted.file_id = p.file_id AND wanted.number = p.number \
         ORDER BY p.file_id, p.number",
        file_select("f_")
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
            })
        })
        .collect()
}

impl<H: DriveHost> Persistence for PageStore<H> {
    type Aggregate = PageRecord<H>;
    type Key = PageKey;
    type Event = ();

    const STYLE: PersistenceStyle = PersistenceStyle::Crud;

    fn load<'a>(
        conn: &'a mut PgConnection,
        key: &'a PageKey,
    ) -> BoxFuture<'a, Result<Option<PageRecord<H>>, EngineError>> {
        Box::pin(async move { Ok(load_pages(conn, std::slice::from_ref(key)).await?.pop()) })
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
        _conn: &'a mut PgConnection,
        _page: &'a PageRecord<H>,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async { Err(read_only()) })
    }

    fn create<'a>(
        _conn: &'a mut PgConnection,
        _page: &'a PageRecord<H>,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async { Err(read_only()) })
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
    let Some(file) = <FileStore<H> as Persistence>::load(conn, &file_id).await? else {
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
                keys = super::store::page_numbers(&mut conn, file_id)
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
        "SELECT number, markdown, origin, updated_at FROM drive.file_page \
         WHERE file_id = $1 ORDER BY number",
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
