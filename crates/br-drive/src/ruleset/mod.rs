//! Processing rules: the rule a host's managers declare, its matching and its
//! roots. One rule says "when *trigger* happens on a file whose media type
//! matches *media_types*, run *steps* in order".

mod gestures;
mod store;
mod view;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use service_engine::JsonScalar;
use service_engine::name::NounName;
use service_engine::wire::Noun;
use uuid::Uuid;

use crate::file::UnknownDbValue;

pub use gestures::{
    CreateRuleset, DeleteRuleset, RulesetSaved, UpdateRuleset, create_ruleset, delete_ruleset,
    update_ruleset,
};
pub use store::select_ruleset;
pub use view::{DriveRuleset, DriveRulesets};

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum RulesetCause {
    Saved { unknown_runner_types: Vec<String> },
    Deleted,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

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
}
