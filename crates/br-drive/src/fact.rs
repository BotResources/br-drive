//! The audit trail (`drive.fact`): what happened to a file, a page, a label
//! or a rule, by whom — append-only, never rewritten. The last change of an
//! object (`updatedAt`, a page's `updatedBy`) is read from its facts; nothing
//! overwrites a "last changed" column any more.

use br_core_integration::Actor;
use chrono::{DateTime, Utc};
use serde::Serialize;
use service_engine::error::EngineError;
use service_engine::pipeline::Reaction;
use sqlx::PgConnection;
use uuid::Uuid;

use crate::host::DriveHost;

/// Who acted, as a fact records it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ActorKind {
    Human,
    Service,
    Runner,
}

impl ActorKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Human => "human",
            Self::Service => "service",
            Self::Runner => "runner",
        }
    }
}

/// The hand behind a gesture and the correlation its facts share.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Author {
    actor_id: Uuid,
    kind: ActorKind,
    impersonator_id: Option<Uuid>,
    correlation_id: Uuid,
}

impl Author {
    /// The principal of a mutation: the effective identity, its kind (a
    /// service account holding the host's runner scope is a runner), and the
    /// admin behind an impersonated session.
    pub(crate) fn of<H: DriveHost>(principal: &H) -> Self {
        let passport = principal.passport();
        let kind = if passport.service_account_id().is_none() {
            ActorKind::Human
        } else if principal.is_runner() {
            ActorKind::Runner
        } else {
            ActorKind::Service
        };
        Self {
            actor_id: passport.actor_id(),
            kind,
            impersonator_id: passport.impersonator_id(),
            correlation_id: Uuid::now_v7(),
        }
    }

    /// The sender of a reaction's message (a Jobs fact, the library's own
    /// command), with the message's correlation when it carries one.
    pub(crate) fn of_reaction(cx: &Reaction<'_>) -> Self {
        let (actor_id, kind) = match cx.actor() {
            Some(actor @ Actor::Human(_)) => (actor.id(), ActorKind::Human),
            Some(actor @ Actor::Service(_)) => (actor.id(), ActorKind::Service),
            None => (Uuid::nil(), ActorKind::Service),
        };
        Self {
            actor_id,
            kind,
            impersonator_id: None,
            correlation_id: cx.correlation_id().unwrap_or_else(|| cx.message_id()),
        }
    }
}

/// What a fact is about.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Subject<'a> {
    File(Uuid),
    Files(&'a [Uuid]),
    Pages { file_id: Uuid, numbers: &'a [i32] },
    Label(Uuid),
    Ruleset(Uuid),
}

/// Records `cause` as a fact of `subject` (one row per file or page), by
/// `author`, at `at`. The fact's type is the cause's `kind`.
pub(crate) async fn record(
    conn: &mut PgConnection,
    author: &Author,
    subject: Subject<'_>,
    cause: impl Serialize,
    at: DateTime<Utc>,
) -> Result<(), EngineError> {
    let payload = serde_json::to_value(cause).map_err(|source| EngineError::Encode {
        what: "a fact's payload",
        source,
    })?;
    let fact_type = payload
        .get("kind")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("Changed")
        .to_string();
    let (aggregate_type, ids, numbers): (&str, Vec<Uuid>, Vec<Option<i32>>) = match subject {
        Subject::File(id) => ("file", vec![id], vec![None]),
        Subject::Files(ids) => ("file", ids.to_vec(), vec![None; ids.len()]),
        Subject::Pages { file_id, numbers } => (
            "page",
            vec![file_id; numbers.len()],
            numbers.iter().copied().map(Some).collect(),
        ),
        Subject::Label(id) => ("label", vec![id], vec![None]),
        Subject::Ruleset(id) => ("ruleset", vec![id], vec![None]),
    };
    if ids.is_empty() {
        return Ok(());
    }
    let fact_ids: Vec<Uuid> = ids.iter().map(|_| Uuid::now_v7()).collect();
    sqlx::query(
        "INSERT INTO drive.fact (id, aggregate_type, aggregate_id, page_number, correlation_id, \
           fact_type, payload, actor_id, actor_kind, impersonator_id, occurred_at) \
         SELECT fact.id, $2, fact.aggregate_id, fact.page_number, $5, $6, $7, $8, $9, $10, $11 \
         FROM unnest($1::uuid[], $3::uuid[], $4::int[]) AS fact(id, aggregate_id, page_number)",
    )
    .bind(&fact_ids)
    .bind(aggregate_type)
    .bind(&ids)
    .bind(&numbers)
    .bind(author.correlation_id)
    .bind(fact_type)
    .bind(payload)
    .bind(author.actor_id)
    .bind(author.kind.as_str())
    .bind(author.impersonator_id)
    .bind(at)
    .execute(conn)
    .await?;
    Ok(())
}

/// The last change of a page (`file_id`, `number` as SQL expressions), as a
/// lateral subquery aliased `alias`.
pub(crate) fn last_page_change(alias: &str, file_id: &str, number: &str) -> String {
    format!(
        "LEFT JOIN LATERAL (SELECT x.occurred_at, x.actor_id FROM drive.fact x \
           WHERE x.aggregate_type = 'page' AND x.aggregate_id = {file_id} \
             AND x.page_number = {number} \
           ORDER BY x.occurred_at DESC, x.id DESC LIMIT 1) {alias} ON true"
    )
}
