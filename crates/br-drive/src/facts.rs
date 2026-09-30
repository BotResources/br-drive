//! What the library tells its host happened: every change of a library state
//! row is a fact the host keeps in its own fact table, handed over through
//! [`DriveHost::record_facts`] in the transaction that writes the state. The
//! library keeps no fact table of its own.
//!
//! The aggregates are soft-EDA, the engine's way: each state-changing method
//! pushes one event and bumps the aggregate's `version`; the store's `save` /
//! `create` writes the state row, then turns the pending events into facts —
//! `seq = base_version + offset + 1`, gap-free per `(noun, key)` — and hands
//! them to the host. The bulk writes that change many rows without loading an
//! aggregate bump each row's `version` in SQL and hand one fact per row.

use br_core_integration::Actor;
use chrono::{DateTime, Utc};
use serde::Serialize;
use service_engine::error::EngineError;
use service_engine::persistence::Aggregate;
use service_engine::pipeline::{Ops, Reaction};
use sqlx::PgConnection;
use uuid::Uuid;

use crate::host::DriveHost;

/// Who acted, as a fact records it (aligned on `br-core-events`'
/// `EventMetadata`): a person, or a service account — a runner is a service
/// account holding the host's runner scope, told by [`FactMeta::is_runner`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ActorKind {
    Human,
    Service,
}

impl ActorKind {
    /// `human` or `service`, as the reference fact table checks it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Human => "human",
            Self::Service => "service",
        }
    }
}

/// The hand behind a fact and the gesture it belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FactMeta {
    /// The effective identity: the principal of a mutation, the sender of a
    /// reaction's message (the nil id when the message names none).
    pub actor_id: Uuid,
    pub actor_kind: ActorKind,
    /// A service account holding the host's runner scope.
    pub is_runner: bool,
    /// The admin behind an impersonated session: the trail never loses the
    /// real hand.
    pub impersonator_id: Option<Uuid>,
    /// The gesture or the message: every fact one gesture wrote shares it (a
    /// folder move's files, a report's pages).
    pub correlation_id: Uuid,
    /// The inbound message a reaction handles; `None` for a mutation.
    pub causation_id: Option<Uuid>,
    /// The engine clock of the gesture.
    pub occurred_at: DateTime<Utc>,
}

impl FactMeta {
    /// The principal of a mutation, at `at`: its effective identity, whether
    /// it is a runner, the admin behind an impersonated session, and a fresh
    /// correlation shared by every fact of the gesture.
    pub fn of<H: DriveHost>(principal: &H, at: DateTime<Utc>) -> Self {
        let passport = principal.passport();
        let service = passport.service_account_id().is_some();
        Self {
            actor_id: passport.actor_id(),
            actor_kind: if service {
                ActorKind::Service
            } else {
                ActorKind::Human
            },
            is_runner: service && principal.is_runner(),
            impersonator_id: passport.impersonator_id(),
            correlation_id: Uuid::now_v7(),
            causation_id: None,
            occurred_at: at,
        }
    }

    /// The sender of a reaction's message (a Jobs fact, the library's own
    /// command), with the message's correlation when it carries one, the
    /// message itself as the cause.
    pub fn of_reaction(cx: &Reaction<'_>) -> Self {
        let (actor_id, actor_kind) = match cx.actor() {
            Some(actor @ Actor::Human(_)) => (actor.id(), ActorKind::Human),
            Some(actor @ Actor::Service(_)) => (actor.id(), ActorKind::Service),
            None => (Uuid::nil(), ActorKind::Service),
        };
        Self {
            actor_id,
            actor_kind,
            is_runner: false,
            impersonator_id: None,
            correlation_id: cx.correlation_id().unwrap_or_else(|| cx.message_id()),
            causation_id: Some(cx.message_id()),
            occurred_at: cx.now().as_datetime(),
        }
    }
}

/// One fact handed to the host: an event of one of the library's aggregates.
#[derive(Debug, Clone, PartialEq)]
pub struct DriveFact {
    /// The aggregate's noun: `drive_file`, `drive_page`, `drive_label`,
    /// `drive_ruleset` or `drive_file_processing`.
    pub noun: &'static str,
    /// The aggregate's key: a UUID, or `{fileId, number}` for a page.
    pub key: serde_json::Value,
    /// The aggregate's version after this event: 1, 2, 3… per `(noun, key)`,
    /// gap-free.
    pub seq: i64,
    /// The schema version of the payload.
    pub version: i32,
    /// The event's `kind`.
    pub event_type: &'static str,
    /// The event, tagged by `kind`.
    pub payload: serde_json::Value,
    pub meta: FactMeta,
}

/// An event of one of the library's aggregates.
pub(crate) trait DriveEvent: Serialize {
    /// The noun the event is a fact of.
    const NOUN: &'static str;
    /// The schema version of the payload.
    const VERSION: i32;
    /// The event's `kind`, as its serde tag names it.
    fn kind(&self) -> &'static str;
}

/// An event with the hand behind it, pending on its aggregate until the store
/// hands it to the host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stamped<E> {
    pub event: E,
    pub meta: FactMeta,
}

/// The pending events of an aggregate: pushing one bumps the version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Pending<E> {
    events: Vec<Stamped<E>>,
}

impl<E> Default for Pending<E> {
    fn default() -> Self {
        Self { events: Vec::new() }
    }
}

impl<E> Pending<E> {
    pub(crate) fn push(&mut self, version: &mut i64, event: E, meta: &FactMeta) {
        *version += 1;
        self.events.push(Stamped { event, meta: *meta });
    }

    pub(crate) fn as_slice(&self) -> &[Stamped<E>] {
        &self.events
    }

    pub(crate) fn len(&self) -> i64 {
        i64::try_from(self.events.len()).unwrap_or(i64::MAX)
    }

    /// Forgets the events once the store handed them: a later save of the
    /// same aggregate in the gesture hands only what came after.
    pub(crate) fn clear(&mut self) {
        self.events.clear();
    }
}

/// A soft-EDA aggregate of the library: its pending events are forgotten once
/// saved, so saving it again in the same gesture hands only the new ones.
pub(crate) trait SoftEda: Aggregate {
    fn clear_pending(&mut self);
}

/// Saves `aggregate` through the engine (its store writes the state row and
/// hands the pending events to the host), then forgets those events.
pub(crate) async fn save<A: SoftEda>(
    ops: &mut Ops<'_>,
    aggregate: &mut A,
) -> Result<(), EngineError> {
    ops.save(&*aggregate).await?;
    aggregate.clear_pending();
    Ok(())
}

/// Creates `aggregate` through the engine, then forgets its pending events.
pub(crate) async fn create<A: SoftEda>(
    ops: &mut Ops<'_>,
    aggregate: &mut A,
) -> Result<(), EngineError> {
    ops.create(&*aggregate).await?;
    aggregate.clear_pending();
    Ok(())
}

fn encode<E: Serialize>(event: &E) -> Result<serde_json::Value, EngineError> {
    serde_json::to_value(event).map_err(|source| EngineError::Encode {
        what: "a drive fact's payload",
        source,
    })
}

/// The key of a fact about a UUID-keyed aggregate.
pub(crate) fn uuid_key(id: Uuid) -> serde_json::Value {
    serde_json::Value::String(id.to_string())
}

/// The facts of `events`, pending on the aggregate `key` whose version before
/// them was `base`.
pub(crate) fn facts_of<E: DriveEvent>(
    key: &serde_json::Value,
    base: i64,
    events: &[Stamped<E>],
) -> Result<Vec<DriveFact>, EngineError> {
    events
        .iter()
        .enumerate()
        .map(|(offset, stamped)| {
            Ok(DriveFact {
                noun: E::NOUN,
                key: key.clone(),
                seq: base + i64::try_from(offset).unwrap_or(i64::MAX) + 1,
                version: E::VERSION,
                event_type: stamped.event.kind(),
                payload: encode(&stamped.event)?,
                meta: stamped.meta,
            })
        })
        .collect()
}

/// One fact of `event` on the aggregate `key`, now at version `seq` — for the
/// bulk writes, which bump the row's version in SQL.
pub(crate) fn fact_at<E: DriveEvent>(
    key: serde_json::Value,
    seq: i64,
    event: &E,
    meta: &FactMeta,
) -> Result<DriveFact, EngineError> {
    Ok(DriveFact {
        noun: E::NOUN,
        key,
        seq,
        version: E::VERSION,
        event_type: event.kind(),
        payload: encode(event)?,
        meta: *meta,
    })
}

/// Hands `facts` to the host, in the caller's transaction. Nothing to hand is
/// no call.
pub(crate) async fn hand<H: DriveHost>(
    conn: &mut PgConnection,
    facts: &[DriveFact],
) -> Result<(), EngineError> {
    if facts.is_empty() {
        return Ok(());
    }
    H::record_facts(conn, facts).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize)]
    #[serde(tag = "kind")]
    enum Sample {
        Made { n: i32 },
        Gone,
    }

    impl DriveEvent for Sample {
        const NOUN: &'static str = "sample";
        const VERSION: i32 = 3;
        fn kind(&self) -> &'static str {
            match self {
                Self::Made { .. } => "Made",
                Self::Gone => "Gone",
            }
        }
    }

    fn meta() -> FactMeta {
        FactMeta {
            actor_id: Uuid::now_v7(),
            actor_kind: ActorKind::Human,
            is_runner: false,
            impersonator_id: None,
            correlation_id: Uuid::now_v7(),
            causation_id: None,
            occurred_at: Utc::now(),
        }
    }

    #[test]
    fn pending_events_number_their_facts_from_the_base_version() {
        let mut version = 4;
        let mut pending = Pending::default();
        let meta = meta();
        pending.push(&mut version, Sample::Made { n: 1 }, &meta);
        pending.push(&mut version, Sample::Gone, &meta);
        assert_eq!(version, 6);
        let base = version - pending.len();
        let key = uuid_key(Uuid::nil());
        let facts = facts_of(&key, base, pending.as_slice()).unwrap();
        assert_eq!(
            facts.iter().map(|fact| fact.seq).collect::<Vec<_>>(),
            vec![5, 6]
        );
        assert_eq!(facts[0].event_type, "Made");
        assert_eq!(
            facts[0].payload,
            serde_json::json!({ "kind": "Made", "n": 1 })
        );
        assert_eq!(facts[1].event_type, "Gone");
        assert_eq!(facts[1].version, 3);
        assert_eq!(facts[1].noun, "sample");
        pending.clear();
        assert!(pending.as_slice().is_empty());
    }

    #[test]
    fn the_actor_kind_is_named_as_the_reference_table_checks_it() {
        assert_eq!(ActorKind::Human.as_str(), "human");
        assert_eq!(ActorKind::Service.as_str(), "service");
    }
}
