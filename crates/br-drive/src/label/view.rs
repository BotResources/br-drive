use std::collections::BTreeSet;
use std::marker::PhantomData;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::impact::Impact;
use service_engine::name::ProjectorName;
use service_engine::population::Population;
use service_engine::projector::Emission;
use service_engine::view::{Populate, Projector};
use service_engine::visibility::Unrestricted;
use uuid::Uuid;

use super::store::{LabelStore, all_ids};
use super::{Label, LabelRow};
use crate::host::{DriveHost, DriveRequest};
use crate::host_window::catalogue_window;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct DriveLabel {
    pub id: Uuid,
    pub name: String,
    pub color: String,
    pub description: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LabelWindow {}

service_engine::open_access!(
    pub LabelAccess = "labels belong to the host service as a whole; the window is gated on the host's ReadLabels gate, never on a cohort"
);

pub struct DriveLabels<H>(PhantomData<fn() -> H>);

impl<H> Default for DriveLabels<H> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<H> DriveLabels<H> {
    pub const NAME: ProjectorName = ProjectorName::from_static("drive_labels");
}

impl<H: DriveHost> Projector for DriveLabels<H> {
    type Principal = H;
    type Noun = Label;
    type Store = LabelStore;
    type Query = LabelWindow;
    type Out = DriveLabel;
    type Visibility = Unrestricted<LabelRow, H, LabelAccess>;

    const NAME: ProjectorName = Self::NAME;

    async fn populate(
        cx: &Populate<'_, H>,
        _query: &LabelWindow,
    ) -> Result<Population<Uuid>, EngineError> {
        if !cx
            .principal()
            .drive_gate(&DriveRequest::ReadLabels)
            .is_allowed()
        {
            return Ok(catalogue_window::<Label>(BTreeSet::new()));
        }
        let mut conn = cx.pool().acquire().await.map_err(EngineError::from)?;
        let keys: BTreeSet<Uuid> = all_ids(&mut conn).await?.into_iter().collect();
        Ok(catalogue_window::<Label>(keys))
    }

    fn project(row: &LabelRow, _principal: &H) -> Result<DriveLabel, EngineError> {
        Ok(DriveLabel {
            id: row.id,
            name: row.name.clone(),
            color: row.color.clone(),
            description: row.description.clone(),
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }

    fn emission(_impact: &Impact) -> Emission {
        Emission::PerImpact
    }
}
