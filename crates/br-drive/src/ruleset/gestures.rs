use futures_util::future::BoxFuture;
use serde::Deserialize;
use service_engine::pipeline::{Mutation, MutationInput, OneShot};
use sqlx::PgConnection;
use uuid::Uuid;

use super::store::serialize_rulesets;
use super::{
    ANY_MEDIA_TYPE, MAX_RULESET_NAME_BYTES, MAX_RULESET_STEPS, MAX_RUNNER_TYPE_BYTES, Ruleset,
    RulesetCause, RulesetRow, RulesetStep, Trigger,
};
use crate::catalogue;
use crate::fault::{DriveFault, codes};
use crate::host::{DriveHost, DriveRequest};
use crate::media::MediaType;

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

/// A rule saved before the host's first catalogue scan is kept: its steps are
/// reported as unknown runner types (the save's warning), and a chain that
/// fires before the scan defers its launch instead of failing.
async fn note_unscanned_catalogue(conn: &mut PgConnection) -> Result<(), DriveFault> {
    if !catalogue::scanned(conn).await? {
        tracing::warn!(
            "a ruleset was saved on a host where no runner-type catalogue scan has completed \
             yet; start `br_drive::watch_runner_types` next to the engine"
        );
    }
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
        note_unscanned_catalogue(cx.connection()).await?;
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
        let unknown_runner_types =
            catalogue::inactive_among(cx.connection(), &ruleset.runner_types()).await?;
        cx.impact_caused::<Ruleset, _>(
            &ruleset.id,
            RulesetCause::Saved {
                unknown_runner_types: unknown_runner_types.clone(),
            },
        )?;
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
        note_unscanned_catalogue(cx.connection()).await?;
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
        let unknown_runner_types =
            catalogue::inactive_among(cx.connection(), &ruleset.runner_types()).await?;
        cx.impact_caused::<Ruleset, _>(
            &ruleset.id,
            RulesetCause::Saved {
                unknown_runner_types: unknown_runner_types.clone(),
            },
        )?;
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
        cx.impact_caused::<Ruleset, _>(&ruleset.id, RulesetCause::Deleted)?;
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_object() -> serde_json::Value {
        serde_json::Value::Object(serde_json::Map::new())
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
