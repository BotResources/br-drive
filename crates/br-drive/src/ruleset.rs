use std::collections::BTreeSet;
use std::marker::PhantomData;

use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::JsonScalar;
use service_engine::error::EngineError;
use service_engine::name::{NounName, ProjectorName};
use service_engine::persistence::{Aggregate, Persistence, PersistenceStyle};
use service_engine::pipeline::{Mutation, MutationInput, OneShot};
use service_engine::population::Population;
use service_engine::view::{Populate, Projector};
use service_engine::visibility::Unrestricted;
use service_engine::wire::Noun;
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::catalogue;
use crate::fault::{DriveFault, codes};
use crate::file::UnknownDbValue;
use crate::host::{DriveHost, DriveRequest};
use crate::media::MediaType;

pub const MAX_RULESET_NAME_BYTES: usize = 255;
pub const MAX_RUNNER_TYPE_BYTES: usize = 128;
pub const MAX_RULESET_STEPS: usize = 32;
pub const ANY_MEDIA_TYPE: &str = "*";

pub struct Ruleset;

impl Noun for Ruleset {
    type Key = Uuid;
    const NAME: NounName = NounName::from_static("drive_ruleset");
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, async_graphql::Enum)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Trigger {
    Upload,
    Reprocess,
    RegeneratePage,
}

impl Trigger {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Upload => "upload",
            Self::Reprocess => "reprocess",
            Self::RegeneratePage => "regenerate_page",
        }
    }

    pub fn from_db_str(text: &str) -> Result<Self, UnknownDbValue> {
        match text {
            "upload" => Ok(Self::Upload),
            "reprocess" => Ok(Self::Reprocess),
            "regenerate_page" => Ok(Self::RegeneratePage),
            other => Err(UnknownDbValue("trigger", other.to_string())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RulesetStep {
    pub runner_type: String,
    #[serde(default = "empty_object")]
    pub options: serde_json::Value,
}

fn empty_object() -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::new())
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct DriveStep {
    pub runner_type: String,
    pub options: JsonScalar,
}

impl From<&RulesetStep> for DriveStep {
    fn from(step: &RulesetStep) -> Self {
        Self {
            runner_type: step.runner_type.clone(),
            options: async_graphql::Json(step.options.clone()),
        }
    }
}

#[derive(Debug, Clone, async_graphql::InputObject)]
pub struct RulesetStepInput {
    pub runner_type: String,
    pub options: Option<JsonScalar>,
}

impl From<RulesetStepInput> for RulesetStep {
    fn from(input: RulesetStepInput) -> Self {
        Self {
            runner_type: input.runner_type,
            options: input
                .options
                .map(|json| json.0)
                .unwrap_or_else(empty_object),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RulesetRow {
    pub id: Uuid,
    pub name: String,
    pub trigger: Trigger,
    pub media_types: Vec<String>,
    pub steps: Vec<RulesetStep>,
    pub is_default: bool,
    pub created_by: Uuid,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Match {
    Exact,
    TypeWildcard,
    Any,
}

fn match_rank(pattern: &str, media_type: &str) -> Option<Match> {
    if pattern == ANY_MEDIA_TYPE {
        return Some(Match::Any);
    }
    if let Some(kind) = pattern.strip_suffix("/*") {
        return media_type
            .split_once('/')
            .is_some_and(|(found, _)| found == kind)
            .then_some(Match::TypeWildcard);
    }
    (pattern == media_type).then_some(Match::Exact)
}

impl RulesetRow {
    pub fn best_match(&self, media_type: &str) -> Option<u8> {
        self.media_types
            .iter()
            .filter_map(|pattern| match_rank(pattern, media_type))
            .min()
            .map(|rank| rank as u8)
    }

    pub fn runner_types(&self) -> Vec<String> {
        self.steps
            .iter()
            .map(|step| step.runner_type.clone())
            .collect()
    }
}

const COLUMNS: &str =
    "id, name, trigger, media_types, steps, is_default, created_by, created_at, updated_at";

fn row_to_ruleset(row: &sqlx::postgres::PgRow) -> Result<RulesetRow, EngineError> {
    let trigger: String = row.get("trigger");
    let steps: serde_json::Value = row.get("steps");
    Ok(RulesetRow {
        id: row.get("id"),
        name: row.get("name"),
        trigger: Trigger::from_db_str(&trigger).map_err(|e| EngineError::Config(e.to_string()))?,
        media_types: row.get("media_types"),
        steps: serde_json::from_value(steps)
            .map_err(|e| EngineError::Config(format!("a ruleset's steps do not decode: {e}")))?,
        is_default: row.get("is_default"),
        created_by: row.get("created_by"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

pub struct RulesetStore;

impl Persistence for RulesetStore {
    type Aggregate = RulesetRow;
    type Key = Uuid;
    type Event = ();

    const STYLE: PersistenceStyle = PersistenceStyle::Crud;

    fn load<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<Option<RulesetRow>, EngineError>> {
        Box::pin(async move {
            let row = sqlx::query(&format!(
                "SELECT {COLUMNS} FROM drive.ruleset WHERE id = $1"
            ))
            .bind(key)
            .fetch_optional(conn)
            .await?;
            row.as_ref().map(row_to_ruleset).transpose()
        })
    }

    fn lock<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Self::row_lock(conn, "drive.ruleset", key)
    }

    fn read_many<'a>(
        conn: &'a mut PgConnection,
        keys: &'a [Uuid],
    ) -> BoxFuture<'a, Result<Vec<(Uuid, RulesetRow)>, EngineError>> {
        Box::pin(async move {
            let rows = sqlx::query(&format!(
                "SELECT {COLUMNS} FROM drive.ruleset WHERE id = ANY($1)"
            ))
            .bind(keys)
            .fetch_all(conn)
            .await?;
            rows.iter()
                .map(|row| row_to_ruleset(row).map(|ruleset| (ruleset.id, ruleset)))
                .collect()
        })
    }

    fn save<'a>(
        conn: &'a mut PgConnection,
        ruleset: &'a RulesetRow,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(
                "UPDATE drive.ruleset SET name = $2, trigger = $3, media_types = $4, steps = $5, \
                   is_default = $6, updated_at = $7 WHERE id = $1",
            )
            .bind(ruleset.id)
            .bind(&ruleset.name)
            .bind(ruleset.trigger.as_str())
            .bind(&ruleset.media_types)
            .bind(
                serde_json::to_value(&ruleset.steps).map_err(|source| EngineError::Encode {
                    what: "ruleset steps",
                    source,
                })?,
            )
            .bind(ruleset.is_default)
            .bind(ruleset.updated_at)
            .execute(conn)
            .await?;
            Ok(())
        })
    }

    fn create<'a>(
        conn: &'a mut PgConnection,
        ruleset: &'a RulesetRow,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(&format!(
                "INSERT INTO drive.ruleset ({COLUMNS}) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
            ))
            .bind(ruleset.id)
            .bind(&ruleset.name)
            .bind(ruleset.trigger.as_str())
            .bind(&ruleset.media_types)
            .bind(
                serde_json::to_value(&ruleset.steps).map_err(|source| EngineError::Encode {
                    what: "ruleset steps",
                    source,
                })?,
            )
            .bind(ruleset.is_default)
            .bind(ruleset.created_by)
            .bind(ruleset.created_at)
            .bind(ruleset.updated_at)
            .execute(conn)
            .await?;
            Ok(())
        })
    }

    fn delete<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query("DELETE FROM drive.ruleset WHERE id = $1")
                .bind(key)
                .execute(conn)
                .await?;
            Ok(())
        })
    }
}

impl Aggregate for RulesetRow {
    type Store = RulesetStore;

    fn key(&self) -> Uuid {
        self.id
    }
}

pub async fn all_ids(conn: &mut PgConnection) -> Result<Vec<Uuid>, EngineError> {
    let rows = sqlx::query("SELECT id FROM drive.ruleset ORDER BY name")
        .fetch_all(conn)
        .await?;
    Ok(rows.iter().map(|row| row.get::<Uuid, _>("id")).collect())
}

async fn all_rulesets(conn: &mut PgConnection) -> Result<Vec<RulesetRow>, EngineError> {
    let rows = sqlx::query(&format!("SELECT {COLUMNS} FROM drive.ruleset"))
        .fetch_all(conn)
        .await?;
    rows.iter().map(row_to_ruleset).collect()
}

async fn serialize_rulesets(conn: &mut PgConnection) -> Result<(), EngineError> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('drive.ruleset'))")
        .execute(conn)
        .await?;
    Ok(())
}

pub async fn select_ruleset(
    conn: &mut PgConnection,
    trigger: Trigger,
    media_type: &MediaType,
    explicit: Option<Uuid>,
) -> Result<Option<RulesetRow>, DriveFault> {
    if let Some(id) = explicit {
        let ruleset = <RulesetStore as Persistence>::load(conn, &id)
            .await?
            .ok_or(DriveFault::Refused(codes::RULESET_NOT_FOUND))?;
        if ruleset.trigger != trigger || ruleset.best_match(media_type.as_str()).is_none() {
            return Err(DriveFault::Refused(codes::RULESET_MISMATCH));
        }
        return Ok(Some(ruleset));
    }
    let candidates = all_rulesets(conn).await?;
    Ok(candidates
        .into_iter()
        .filter(|ruleset| ruleset.is_default && ruleset.trigger == trigger)
        .filter_map(|ruleset| {
            ruleset
                .best_match(media_type.as_str())
                .map(|rank| (rank, ruleset))
        })
        .min_by_key(|(rank, ruleset)| (*rank, ruleset.name.clone()))
        .map(|(_, ruleset)| ruleset))
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct DriveRuleset {
    pub id: Uuid,
    pub name: String,
    pub trigger: Trigger,
    pub media_types: Vec<String>,
    pub steps: Vec<DriveStep>,
    pub is_default: bool,
    pub created_by: Uuid,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RulesetWindow {}

service_engine::open_access!(
    pub RulesetAccess = "rulesets belong to the host service as a whole; the window is gated on the host's ReadRulesets gate, never on a cohort"
);

pub struct DriveRulesets<H>(PhantomData<fn() -> H>);

impl<H> Default for DriveRulesets<H> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<H> DriveRulesets<H> {
    pub const NAME: ProjectorName = ProjectorName::from_static("drive_rulesets");
}

impl<H: DriveHost> Projector for DriveRulesets<H> {
    type Principal = H;
    type Noun = Ruleset;
    type Store = RulesetStore;
    type Query = RulesetWindow;
    type Out = DriveRuleset;
    type Visibility = Unrestricted<RulesetRow, H, RulesetAccess>;

    const NAME: ProjectorName = Self::NAME;

    async fn populate(
        cx: &Populate<'_, H>,
        _query: &RulesetWindow,
    ) -> Result<Population<Uuid>, EngineError> {
        if !cx
            .principal()
            .drive_gate(&DriveRequest::ReadRulesets)
            .is_allowed()
        {
            return Ok(Population::Keys(BTreeSet::new()));
        }
        let mut conn = cx.pool().acquire().await.map_err(EngineError::from)?;
        Ok(Population::Keys(
            all_ids(&mut conn).await?.into_iter().collect(),
        ))
    }

    fn project(row: &RulesetRow, _principal: &H) -> Result<DriveRuleset, EngineError> {
        Ok(DriveRuleset {
            id: row.id,
            name: row.name.clone(),
            trigger: row.trigger,
            media_types: row.media_types.clone(),
            steps: row.steps.iter().map(DriveStep::from).collect(),
            is_default: row.is_default,
            created_by: row.created_by,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, async_graphql::SimpleObject)]
pub struct RulesetSaved {
    pub id: Uuid,
    pub unknown_runner_types: Vec<String>,
}

fn validate_name(name: &str) -> Result<String, DriveFault> {
    let name = name.trim();
    if name.is_empty() || name.len() > MAX_RULESET_NAME_BYTES {
        return Err(DriveFault::Refused(codes::INVALID_RULESET));
    }
    Ok(name.to_string())
}

fn validate_media_types(media_types: &[String]) -> Result<Vec<String>, DriveFault> {
    if media_types.is_empty() {
        return Err(DriveFault::Refused(codes::INVALID_RULESET));
    }
    let mut out = Vec::new();
    for pattern in media_types {
        let sound = pattern == ANY_MEDIA_TYPE
            || pattern
                .strip_suffix("/*")
                .is_some_and(|kind| MediaType::parse(&format!("{kind}/x")).is_ok())
            || MediaType::parse(pattern).is_ok();
        if !sound {
            return Err(DriveFault::Refused(codes::INVALID_MEDIA_TYPE));
        }
        let pattern = pattern.to_ascii_lowercase();
        if !out.contains(&pattern) {
            out.push(pattern);
        }
    }
    Ok(out)
}

fn validate_steps(steps: &[RulesetStep]) -> Result<(), DriveFault> {
    if steps.is_empty() || steps.len() > MAX_RULESET_STEPS {
        return Err(DriveFault::Refused(codes::INVALID_RULESET));
    }
    for step in steps {
        let name = step.runner_type.as_str();
        let sound = !name.is_empty()
            && name.len() <= MAX_RUNNER_TYPE_BYTES
            && name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'));
        if !sound || !step.options.is_object() {
            return Err(DriveFault::Refused(codes::INVALID_RULESET));
        }
    }
    Ok(())
}

async fn require_free_name(
    conn: &mut PgConnection,
    name: &str,
    except: Option<Uuid>,
) -> Result<(), DriveFault> {
    let taken: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM drive.ruleset WHERE lower(name) = lower($1) AND id <> $2)",
    )
    .bind(name)
    .bind(except.unwrap_or(Uuid::nil()))
    .fetch_one(conn)
    .await?;
    if taken {
        return Err(DriveFault::Refused(codes::RULESET_NAME_TAKEN));
    }
    Ok(())
}

async fn require_default_free(
    conn: &mut PgConnection,
    trigger: Trigger,
    media_types: &[String],
    except: Option<Uuid>,
) -> Result<(), DriveFault> {
    let clash: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM drive.ruleset \
         WHERE is_default AND trigger = $1 AND media_types && $2 AND id <> $3)",
    )
    .bind(trigger.as_str())
    .bind(media_types)
    .bind(except.unwrap_or(Uuid::nil()))
    .fetch_one(conn)
    .await?;
    if clash {
        return Err(DriveFault::Refused(codes::DEFAULT_ALREADY_SET));
    }
    Ok(())
}

fn manage_gate<H: DriveHost>(principal: &H) -> Result<(), DriveFault> {
    principal
        .drive_gate(&DriveRequest::ManageRulesets)
        .require()?;
    Ok(())
}

#[derive(Debug, Deserialize)]
pub struct CreateRuleset {
    pub id: Uuid,
    pub name: String,
    pub trigger: Trigger,
    pub media_types: Vec<String>,
    pub steps: Vec<RulesetStep>,
    pub is_default: bool,
}

impl MutationInput for CreateRuleset {
    type Output = OneShot<RulesetSaved>;
    type Error = DriveFault;
    const NAME: &'static str = "drive_create_ruleset";
}

pub fn create_ruleset<'m, H: DriveHost>(
    cx: &'m mut Mutation<'m, H>,
    input: CreateRuleset,
) -> BoxFuture<'m, Result<OneShot<RulesetSaved>, DriveFault>> {
    Box::pin(async move {
        manage_gate(cx.principal())?;
        let name = validate_name(&input.name)?;
        let media_types = validate_media_types(&input.media_types)?;
        validate_steps(&input.steps)?;
        serialize_rulesets(cx.connection()).await?;
        require_free_name(cx.connection(), &name, None).await?;
        if input.is_default {
            require_default_free(cx.connection(), input.trigger, &media_types, None).await?;
        }
        let now = cx.now().as_datetime();
        let ruleset = RulesetRow {
            id: input.id,
            name,
            trigger: input.trigger,
            media_types,
            steps: input.steps,
            is_default: input.is_default,
            created_by: cx.principal().id().as_uuid(),
            created_at: now,
            updated_at: now,
        };
        cx.create(&ruleset).await?;
        cx.impact::<Ruleset>(&ruleset.id, service_engine::impact::Dims::EMPTY)?;
        let unknown_runner_types =
            catalogue::inactive_among(cx.connection(), &ruleset.runner_types()).await?;
        Ok(OneShot(RulesetSaved {
            id: ruleset.id,
            unknown_runner_types,
        }))
    })
}

#[derive(Debug, Deserialize)]
pub struct UpdateRuleset {
    pub id: Uuid,
    pub name: Option<String>,
    pub media_types: Option<Vec<String>>,
    pub steps: Option<Vec<RulesetStep>>,
    pub is_default: Option<bool>,
}

impl MutationInput for UpdateRuleset {
    type Output = OneShot<RulesetSaved>;
    type Error = DriveFault;
    const NAME: &'static str = "drive_update_ruleset";
}

pub fn update_ruleset<'m, H: DriveHost>(
    cx: &'m mut Mutation<'m, H>,
    input: UpdateRuleset,
) -> BoxFuture<'m, Result<OneShot<RulesetSaved>, DriveFault>> {
    Box::pin(async move {
        manage_gate(cx.principal())?;
        serialize_rulesets(cx.connection()).await?;
        let mut ruleset = cx
            .load::<RulesetRow>(&input.id)
            .await?
            .ok_or(DriveFault::Refused(codes::RULESET_NOT_FOUND))?;
        let name = match input.name.as_deref() {
            Some(name) => validate_name(name)?,
            None => ruleset.name.clone(),
        };
        let media_types = match input.media_types.as_deref() {
            Some(media_types) => validate_media_types(media_types)?,
            None => ruleset.media_types.clone(),
        };
        let steps = input.steps.unwrap_or_else(|| ruleset.steps.clone());
        validate_steps(&steps)?;
        let is_default = input.is_default.unwrap_or(ruleset.is_default);
        let unchanged = name == ruleset.name
            && media_types == ruleset.media_types
            && steps == ruleset.steps
            && is_default == ruleset.is_default;
        if unchanged {
            return Err(DriveFault::Refused(codes::NOTHING_TO_CHANGE));
        }
        require_free_name(cx.connection(), &name, Some(ruleset.id)).await?;
        if is_default {
            require_default_free(
                cx.connection(),
                ruleset.trigger,
                &media_types,
                Some(ruleset.id),
            )
            .await?;
        }
        ruleset.name = name;
        ruleset.media_types = media_types;
        ruleset.steps = steps;
        ruleset.is_default = is_default;
        ruleset.updated_at = cx.now().as_datetime();
        cx.save(&ruleset).await?;
        cx.impact::<Ruleset>(&ruleset.id, service_engine::impact::Dims::EMPTY)?;
        let unknown_runner_types =
            catalogue::inactive_among(cx.connection(), &ruleset.runner_types()).await?;
        Ok(OneShot(RulesetSaved {
            id: ruleset.id,
            unknown_runner_types,
        }))
    })
}

#[derive(Debug, Deserialize)]
pub struct DeleteRuleset {
    pub id: Uuid,
}

impl MutationInput for DeleteRuleset {
    type Output = ();
    type Error = DriveFault;
    const NAME: &'static str = "drive_delete_ruleset";
}

pub fn delete_ruleset<'m, H: DriveHost>(
    cx: &'m mut Mutation<'m, H>,
    input: DeleteRuleset,
) -> BoxFuture<'m, Result<(), DriveFault>> {
    Box::pin(async move {
        manage_gate(cx.principal())?;
        let ruleset = cx
            .load::<RulesetRow>(&input.id)
            .await?
            .ok_or(DriveFault::Refused(codes::RULESET_NOT_FOUND))?;
        cx.delete(&ruleset).await?;
        cx.impact::<Ruleset>(&ruleset.id, service_engine::impact::Dims::EMPTY)?;
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ruleset(patterns: &[&str]) -> RulesetRow {
        RulesetRow {
            id: Uuid::now_v7(),
            name: "r".into(),
            trigger: Trigger::Upload,
            media_types: patterns.iter().map(|p| p.to_string()).collect(),
            steps: vec![],
            is_default: true,
            created_by: Uuid::now_v7(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn an_exact_media_type_outranks_a_type_wildcard_which_outranks_the_catch_all() {
        assert_eq!(
            ruleset(&["application/pdf"]).best_match("application/pdf"),
            Some(Match::Exact as u8)
        );
        assert_eq!(
            ruleset(&["application/*"]).best_match("application/pdf"),
            Some(Match::TypeWildcard as u8)
        );
        assert_eq!(
            ruleset(&["*"]).best_match("application/pdf"),
            Some(Match::Any as u8)
        );
        assert_eq!(ruleset(&["text/*"]).best_match("application/pdf"), None);
        assert_eq!(
            ruleset(&["*", "application/pdf"]).best_match("application/pdf"),
            Some(Match::Exact as u8)
        );
    }

    #[test]
    fn media_type_patterns_are_validated_and_lowercased() {
        assert_eq!(
            validate_media_types(&["Application/PDF".into(), "text/*".into(), "*".into()]).unwrap(),
            vec!["application/pdf", "text/*", "*"]
        );
        assert!(validate_media_types(&[]).is_err());
        assert!(validate_media_types(&["pdf".into()]).is_err());
        assert!(validate_media_types(&["*/pdf".into()]).is_err());
    }

    #[test]
    fn steps_need_a_sound_runner_type_and_object_options() {
        let sound = RulesetStep {
            runner_type: "pdf-local".into(),
            options: empty_object(),
        };
        assert!(validate_steps(std::slice::from_ref(&sound)).is_ok());
        assert!(validate_steps(&[]).is_err());
        assert!(
            validate_steps(&[RulesetStep {
                runner_type: "has space".into(),
                options: empty_object(),
            }])
            .is_err()
        );
        assert!(
            validate_steps(&[RulesetStep {
                runner_type: "ok".into(),
                options: serde_json::json!([1]),
            }])
            .is_err()
        );
    }
}
