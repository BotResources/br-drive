use std::time::Duration;

use br_drive::{DriveFact, DriveHost, DrivePath, DriveRequest};
use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::gate::{Gate, Reason};
use service_engine::impact::Deps;
use service_engine::pipeline::Ops;
use service_engine::principal::Principal;
use sqlx::PgConnection;
use uuid::Uuid;

use crate::kernel::facts::HostSettings;
use crate::kernel::{AppPrincipal, OWNERSHIP_DEP};

pub const NOT_THE_OWNER: Reason = Reason::new("NOT_THE_WORKSPACE_OWNER");
pub const UNRENDERABLE_MEDIA_TYPE: Reason = Reason::new("UNRENDERABLE_MEDIA_TYPE");
pub const FORBIDDEN_FOLDER: Reason = Reason::new("FORBIDDEN_FOLDER");
pub const NOT_THE_UPLOADER: Reason = Reason::new("NOT_THE_UPLOADER");
/// The scope of a host clean-up account: it may ask for a folder gesture in
/// any workspace, but no single file is its to move or delete — so the
/// library refuses its folder gestures whole.
pub const SWEEP_SCOPE: &str = "workspace:sweep";
/// The host's own per-file rule: a file whose metadata says `{"hold": true}` may be neither moved nor deleted.
pub const FILE_ON_HOLD: Reason = Reason::new("FILE_ON_HOLD");
pub const HOLD_KEY: &str = "hold";

pub const UNRENDERABLE: &str = "application/x-unrenderable";
pub const FORBIDDEN_PREFIX: &str = "forbidden";
pub const MANAGE_SCOPE: &str = "workspace:manage";
pub const NOT_A_MANAGER: Reason = Reason::new("NOT_A_WORKSPACE_MANAGER");
pub const DISPLAY_NAME_CLAIM: &str = "name";
/// A title the host's fact table refuses to record: retitling a file to it
/// fails the gesture, which the suite uses to prove the state change rolls
/// back with the facts.
pub const UNRECORDABLE_TITLE: &str = "unrecordable";
/// A runner type the host's fact table refuses to see a job created for: the
/// suite's proof that a commit starting a chain rolls back whole.
pub const UNRECORDABLE_RUNNER_TYPE: &str = "unrecordable";
pub const FACT_REFUSED: Reason = Reason::new("FACT_REFUSED");

impl DriveHost for AppPrincipal {
    const SERVICE: &'static str = crate::SERVICE;

    const RUNNER_SCOPE: &'static str = "workspace:runner";

    const VISIBILITY_DEPS: Deps = Deps::from_bits(1 << OWNERSHIP_DEP);

    const SOURCE_ORPHAN_AFTER: Duration = Duration::from_secs(8);

    const IMAGE_ORPHAN_AFTER: Duration = Duration::from_secs(8);

    const IMAGE_MAX_BYTES: u64 = 1 << 20;

    const BULK_RESET_THRESHOLD: usize = 3;

    #[cfg(feature = "workspace")]
    type DriveOwner = crate::slices::workspace::Workspace;
    #[cfg(not(feature = "workspace"))]
    type DriveOwner = br_drive::NoDriveOwner;

    fn drive_gate(&self, request: &DriveRequest<'_, Self>) -> Gate {
        if let DriveRequest::CreateFile { media_type, .. } = request
            && media_type.as_str() == UNRENDERABLE
        {
            return Gate::blocked(UNRENDERABLE_MEDIA_TYPE);
        }
        match request {
            DriveRequest::ManageRulesets | DriveRequest::ManageLabels => {
                return if self.holds_scope(MANAGE_SCOPE) {
                    Gate::allowed()
                } else {
                    Gate::blocked(NOT_A_MANAGER)
                };
            }
            DriveRequest::ReadRulesets => {
                return if self.is_service() {
                    Gate::blocked(NOT_A_MANAGER)
                } else {
                    Gate::allowed()
                };
            }
            DriveRequest::MoveFolder { .. } | DriveRequest::DeleteFolder { .. }
                if self.is_service() && self.holds_scope(SWEEP_SCOPE) =>
            {
                return Gate::allowed();
            }
            // The catalogue is for the people of the host, never for a runner.
            DriveRequest::ReadLabels => {
                return if self.is_service() {
                    Gate::blocked(NOT_THE_OWNER)
                } else {
                    Gate::allowed()
                };
            }

            _ => {}
        }
        let owned = request
            .drive()
            .is_some_and(|drive| self.owns_workspace(drive));
        let owned_target = match request {
            DriveRequest::UpdateFile { target_drive, .. } => self.owns_workspace(*target_drive),
            _ => true,
        };
        if !(owned && owned_target) {
            return Gate::blocked(NOT_THE_OWNER);
        }
        match request {
            // Only the person who uploaded a file may confirm it, even in a
            // workspace that changed hands in between.
            DriveRequest::CommitUpload { file } if file.created_by != self.id().as_uuid() => {
                Gate::blocked(NOT_THE_UPLOADER)
            }
            // A file on hold stays where it is — and so does every folder
            // holding it: the library asks this rule for each file of a folder
            // gesture too.
            DriveRequest::UpdateFile { file, .. } | DriveRequest::DeleteFile { file }
                if file.metadata.get(HOLD_KEY) == Some(&serde_json::Value::Bool(true)) =>
            {
                Gate::blocked(FILE_ON_HOLD)
            }
            _ => Gate::allowed(),
        }
    }

    fn record_facts<'a>(
        conn: &'a mut PgConnection,
        facts: &'a [DriveFact],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(record_facts(conn, facts))
    }

    fn visible_drives(&self) -> Vec<Uuid> {
        self.owned_workspaces()
    }

    fn display_name(&self) -> Option<String> {
        self.passport().claim::<String>(DISPLAY_NAME_CLAIM)
    }

    fn process_on_commit(&self) -> bool {
        self.facts()
            .get::<HostSettings>()
            .is_none_or(|settings| settings.process_on_commit)
    }

    fn upload_window(&self) -> Duration {
        self.facts()
            .get::<HostSettings>()
            .map(|settings| settings.upload_window)
            .unwrap_or(HostSettings::DEFAULT_UPLOAD_WINDOW)
    }

    fn folder_moved<'a, 'o>(
        ops: &'a mut Ops<'o>,
        drive: Uuid,
        old_prefix: &'a DrivePath,
        new_prefix: &'a DrivePath,
    ) -> BoxFuture<'a, Result<(), EngineError>>
    where
        'o: 'a,
    {
        Box::pin(async move {
            refuse_forbidden(new_prefix)?;
            log_folder_gesture(
                ops,
                drive,
                "moved",
                old_prefix.as_str(),
                Some(new_prefix.as_str()),
            )
            .await
        })
    }

    fn folder_deleted<'a, 'o>(
        ops: &'a mut Ops<'o>,
        drive: Uuid,
        prefix: &'a DrivePath,
    ) -> BoxFuture<'a, Result<(), EngineError>>
    where
        'o: 'a,
    {
        Box::pin(async move {
            refuse_forbidden(prefix)?;
            log_folder_gesture(ops, drive, "deleted", prefix.as_str(), None).await
        })
    }
}

/// Inserts `facts` into the host's fact table (`workspace_fact`), one
/// statement for the batch.
pub async fn record_facts(conn: &mut PgConnection, facts: &[DriveFact]) -> Result<(), EngineError> {
    let text = |fact: &DriveFact, field: &str| {
        fact.payload
            .get(field)
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
    };
    let refused = facts.iter().any(|fact| match fact.event_type {
        "Retitled" => text(fact, "to").as_deref() == Some(UNRECORDABLE_TITLE),
        "JobCreated" => text(fact, "runner_type").as_deref() == Some(UNRECORDABLE_RUNNER_TYPE),
        _ => false,
    });
    if refused {
        return Err(EngineError::PolicyRefused {
            code: FACT_REFUSED.code(),
        });
    }
    let mut ids = Vec::with_capacity(facts.len());
    let mut nouns = Vec::with_capacity(facts.len());
    let mut keys = Vec::with_capacity(facts.len());
    let mut seqs = Vec::with_capacity(facts.len());
    let mut versions = Vec::with_capacity(facts.len());
    let mut types = Vec::with_capacity(facts.len());
    let mut payloads = Vec::with_capacity(facts.len());
    let mut actors = Vec::with_capacity(facts.len());
    let mut kinds = Vec::with_capacity(facts.len());
    let mut runners = Vec::with_capacity(facts.len());
    let mut impersonators = Vec::with_capacity(facts.len());
    let mut correlations = Vec::with_capacity(facts.len());
    let mut causations = Vec::with_capacity(facts.len());
    let mut instants = Vec::with_capacity(facts.len());
    for fact in facts {
        ids.push(Uuid::now_v7());
        nouns.push(fact.noun);
        keys.push(fact.key.clone());
        seqs.push(fact.seq);
        versions.push(fact.version);
        types.push(fact.event_type);
        payloads.push(fact.payload.clone());
        actors.push(fact.meta.actor_id);
        kinds.push(fact.meta.actor_kind.as_str());
        runners.push(fact.meta.is_runner);
        impersonators.push(fact.meta.impersonator_id);
        correlations.push(fact.meta.correlation_id);
        causations.push(fact.meta.causation_id);
        instants.push(fact.meta.occurred_at);
    }
    sqlx::query(
        "INSERT INTO workspace_fact (id, noun, key, seq, version, event_type, payload, actor_id, \
           actor_kind, is_runner, impersonator_id, correlation_id, causation_id, occurred_at) \
         SELECT * FROM unnest($1::uuid[], $2::text[], $3::jsonb[], $4::bigint[], $5::int[], \
           $6::text[], $7::jsonb[], $8::uuid[], $9::text[], $10::bool[], $11::uuid[], \
           $12::uuid[], $13::uuid[], $14::timestamptz[])",
    )
    .bind(&ids)
    .bind(&nouns)
    .bind(&keys)
    .bind(&seqs)
    .bind(&versions)
    .bind(&types)
    .bind(&payloads)
    .bind(&actors)
    .bind(&kinds)
    .bind(&runners)
    .bind(&impersonators)
    .bind(&correlations)
    .bind(&causations)
    .bind(&instants)
    .execute(conn)
    .await?;
    Ok(())
}

fn refuse_forbidden(prefix: &DrivePath) -> Result<(), EngineError> {
    if prefix.as_str() == FORBIDDEN_PREFIX {
        return Err(EngineError::PolicyRefused {
            code: FORBIDDEN_FOLDER.code(),
        });
    }
    Ok(())
}

async fn log_folder_gesture(
    ops: &mut Ops<'_>,
    drive: Uuid,
    gesture: &str,
    prefix: &str,
    new_prefix: Option<&str>,
) -> Result<(), EngineError> {
    let now = ops.now().as_datetime();
    sqlx::query(
        "INSERT INTO workspace_folder_gesture (id, workspace_id, gesture, prefix, new_prefix, at) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(Uuid::now_v7())
    .bind(drive)
    .bind(gesture)
    .bind(prefix)
    .bind(new_prefix)
    .bind(now)
    .execute(ops.connection())
    .await?;
    Ok(())
}
