use std::collections::BTreeSet;
use std::marker::PhantomData;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::impact::Impact;
use service_engine::name::ProjectorName;
use service_engine::population::Population;
use service_engine::projector::Emission;
use service_engine::view::{Populate, Projector, windowed};
use service_engine::visibility::Unrestricted;
use uuid::Uuid;

use super::store::{RulesetStore, all_ids};
use super::{DriveStep, Ruleset, RulesetRow, Trigger};
use crate::host::{DriveHost, DriveRequest};

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
        Ok(windowed::<Self>(
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

    fn emission(_impact: &Impact) -> Emission {
        Emission::PerImpact
    }
}
