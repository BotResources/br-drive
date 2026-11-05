use br_core_auth::Passport;
use contract_jobs::command::{CancelJob, FailJob, FinishJob, TriggeredBy};
use contract_jobs::command_v3::CreateJobV3;
use service_engine::BlobRef;
use service_engine::error::EngineError;
use service_engine::impact::Dims;
use service_engine::pipeline::Ops;
use uuid::Uuid;

use super::commands::{Initiator, JobCancel, JobCreate, JobFail, JobFinish};
use super::roots::roots;
use super::state::{CANCELLED, FileProcessing, PASSPORT_MISSING, PASSPORT_UNREADABLE};
use crate::facts::{self, FactMeta};
use crate::fault::{DriveFault, codes};
use crate::file::images::drop_images;
use crate::file::{FileCause, FileEvent, FileRow, Page, PageKey, processed, store};
use crate::host::DriveHost;
use crate::ruleset::{RulesetRow, RulesetStep, Trigger};

fn merge_options(base: &serde_json::Value, extra: &serde_json::Value) -> serde_json::Value {
    let mut merged = match base {
        serde_json::Value::Object(map) => map.clone(),
        _ => serde_json::Map::new(),
    };
    if let serde_json::Value::Object(extra) = extra {
        for (key, value) in extra {
            merged.insert(key.clone(), value.clone());
        }
    }
    serde_json::Value::Object(merged)
}

/// What a chain runs: the rule it came from (none when a file replays its own
/// snapshot) and the steps, with the gesture's options merged into the first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainPlan {
    pub ruleset_id: Option<Uuid>,
    pub steps: Vec<RulesetStep>,
}

impl ChainPlan {
    pub fn from_ruleset(ruleset: &RulesetRow, first_options: Option<&serde_json::Value>) -> Self {
        let mut steps = ruleset.steps.clone();
        if let (Some(first), Some(extra)) = (steps.first_mut(), first_options) {
            first.options = merge_options(&first.options, extra);
        }
        Self {
            ruleset_id: Some(ruleset.id),
            steps,
        }
    }

    pub fn replay<H>(file: &FileRow<H>) -> Option<Self> {
        let steps = file.steps.clone().filter(|steps| !steps.is_empty())?;
        Some(Self {
            ruleset_id: file.ruleset_id,
            steps,
        })
    }
}

/// The file's processing, locked: the stored one, or a blank for a file never
/// processed (saved only once a chain gave it a job).
pub(crate) async fn load_processing<H: DriveHost>(
    cx: &mut Ops<'_>,
    file: &FileRow<H>,
) -> Result<FileProcessing<H>, DriveFault> {
    Ok(match cx.load::<FileProcessing<H>>(&file.id).await? {
        Some(processing) => processing,
        None => FileProcessing::blank(file.id, cx.now().as_datetime()),
    })
}

/// Saves the processing and lets the rest of the gesture read the file as it
/// now is: its status, and its last change.
async fn save_processing<H: DriveHost>(
    cx: &mut Ops<'_>,
    file: &mut FileRow<H>,
    processing: &mut FileProcessing<H>,
) -> Result<(), DriveFault> {
    facts::save(cx, processing).await?;
    file.status = processing.status();
    file.updated_at = file.updated_at.max(processing.updated_at);
    Ok(())
}

/// The Passport the chain acts under, read back from the file's processing
/// for its next step: the `processingError` the chain ends with when it is
/// not held ([`PASSPORT_MISSING`]) or no longer reads as a Passport
/// ([`PASSPORT_UNREADABLE`]).
pub(crate) fn chain_passport(stored: Option<serde_json::Value>) -> Result<Passport, &'static str> {
    let stored = stored.ok_or(PASSPORT_MISSING)?;
    serde_json::from_value::<Passport>(stored).map_err(|_| PASSPORT_UNREADABLE)
}

/// Who Jobs is told started the job: the Passport's own user — Jobs refuses a
/// `triggered_by` that is not (`passport_user_mismatch`) — named as the
/// initiator was when it is that user; nobody under a service Passport.
fn triggered_by(passport: &Passport, initiator: Option<&Initiator>) -> Option<TriggeredBy> {
    let user = passport.user_id()?;
    let display_name = initiator
        .filter(|initiator| initiator.id == user)
        .and_then(|initiator| initiator.display_name.clone());
    Initiator {
        id: user,
        display_name,
    }
    .triggered_by()
}

/// The runner type of step `index` of the file's snapshot; blank past its end.
fn runner_type_of<H>(file: &FileRow<H>, index: usize) -> String {
    file.steps
        .as_ref()
        .and_then(|steps| steps.get(index))
        .map(|step| step.runner_type.clone())
        .unwrap_or_default()
}

fn step_number(index: usize) -> i32 {
    i32::try_from(index).unwrap_or(i32::MAX)
}

/// Starts step `index` of the file's snapshot as job `job_id`: the file's
/// running job and its `job.create` (v3, carrying the chain's `passport`),
/// staged through the outbox. `None` past the last step.
#[allow(clippy::too_many_arguments)]
async fn launch_step<H: DriveHost>(
    cx: &mut Ops<'_>,
    meta: &FactMeta,
    file: &FileRow<H>,
    processing: &mut FileProcessing<H>,
    index: usize,
    job_id: Uuid,
    trigger: Option<Trigger>,
    initiator: Option<Initiator>,
    passport: &Passport,
) -> Result<Option<Uuid>, DriveFault> {
    let steps = file.steps.clone().unwrap_or_default();
    let Some(step) = steps.get(index) else {
        return Ok(None);
    };
    let step_index = i32::try_from(index).map_err(|_| {
        DriveFault::Engine(EngineError::Config("a chain step index overflows".into()))
    })?;
    let roots = roots::<H>()?;
    let config = serde_json::json!({
        "host": H::SERVICE,
        "file_id": file.id,
        "job_id": job_id,
        "context_root": roots.context_root,
        "image_upload_root": roots.image_upload_root,
        "rendition_upload_root": roots.rendition_upload_root,
        "report_root": roots.report_root,
        "failure_root": roots.failure_root,
        "step": index,
        "options": step.options,
    });
    let document = serde_json::to_value(passport).map_err(|error| {
        DriveFault::Engine(EngineError::Config(format!(
            "a passport does not encode: {error}"
        )))
    })?;
    let triggered_by = triggered_by(passport, initiator.as_ref());
    processing.create_job(
        job_id,
        step_index,
        step.runner_type.clone(),
        trigger,
        initiator,
        document.clone(),
        meta,
    );
    // The chain is a host-side sequence correlated by the file id: no step names
    // a parent. Jobs refuses a terminal parent, and a live one would make the
    // step the parent runner's work instead of the host's.
    cx.command(JobCreate {
        payload: CreateJobV3 {
            job_id,
            runner_type: step.runner_type.clone(),
            producer: H::SERVICE.to_string(),
            config: Some(config),
            payload: None,
            passport: Some(document),
            triggered_by,
            parent_job_id: None,
            initial_job_id: None,
            mode: None,
            caller_address: None,
            acting_run_id: None,
            acting_instance_key: None,
            source_bc: Some(H::SERVICE.to_string()),
            source_entity_id: Some(file.id),
            max_attempts: None,
            priority: None,
        },
    })?;
    Ok(Some(job_id))
}

/// Starts a chain on a file just committed, READY or FAILED: the plan becomes
/// the file's snapshot and its first step a job. Nothing of the previous
/// results is touched — they stay readable while the new run reports over
/// them. The chain acts under `passport`, the initiator's own: it is kept with
/// the file's processing until the chain ends, and every step's job carries
/// it. On a source still pending the engine's promotion — its runner could
/// not read it — the first step's job id is minted and the step awaits the
/// promotion (`SourceAwaited`, the file PROCESSING); its `job.create` is
/// staged by [`release_awaited_step`] when the promotion reaches the library.
pub(crate) async fn start_chain<H: DriveHost>(
    cx: &mut Ops<'_>,
    meta: &FactMeta,
    file: &mut FileRow<H>,
    plan: ChainPlan,
    trigger: Trigger,
    initiator: Initiator,
    passport: &Passport,
) -> Result<(), DriveFault> {
    file.ruleset_id = plan.ruleset_id;
    file.steps = Some(plan.steps.clone());
    facts::save(cx, file).await?;
    let mut processing = load_processing(cx, file).await?;
    let document = serde_json::to_value(passport).map_err(|error| {
        DriveFault::Engine(EngineError::Config(format!(
            "a passport does not encode: {error}"
        )))
    })?;
    processing.start_chain(trigger, plan.ruleset_id, plan.steps, document, meta);
    if file.steps.as_ref().is_none_or(Vec::is_empty) {
        return Err(DriveFault::Refused(codes::INVALID_RULESET));
    }
    if super::source_pending(cx.connection(), file.blob_ref).await? {
        let job_id = Uuid::now_v7();
        processing.await_source(
            job_id,
            0,
            runner_type_of(file, 0),
            Some(trigger),
            Some(initiator),
            meta,
        );
        save_processing(cx, file, &mut processing).await?;
        crate::file::file_changed::<H>(cx, file, FileCause::ProcessingStarted { job_id, step: 0 })?;
        return Ok(());
    }
    let Some(job_id) = launch_step(
        cx,
        meta,
        file,
        &mut processing,
        0,
        Uuid::now_v7(),
        Some(trigger),
        Some(initiator),
        passport,
    )
    .await?
    else {
        return Err(DriveFault::Refused(codes::INVALID_RULESET));
    };
    save_processing(cx, file, &mut processing).await?;
    crate::file::file_changed::<H>(cx, file, FileCause::ProcessingStarted { job_id, step: 0 })?;
    Ok(())
}

/// The runner's final report ended the file's running job, in the report's
/// transaction: the job settles and Jobs is told (`job.finish`). Past the last
/// step the chain ends there (READY). Otherwise the next step is decided now —
/// its job id minted and held on the file's processing (`NextStepHeld`), the
/// file still PROCESSING — and its `job.create` waits for Jobs' own end of
/// this job ([`release_next_step`]): Jobs holds one live job per file and
/// judges a create and a finish on separate consumers, in no order, so a
/// create staged beside the finish could be refused `duplicate_active_entity`.
/// A user's cancel that crossed the report stops the chain there: the next
/// step is skipped (`StepSkipped`), never launched, and the file is FAILED
/// `cancelled`; on the last step there is nothing left to stop and the file
/// is READY. The next step's job carries the chain's Passport, read back from
/// the file's processing; held no more or unreadable, the chain stops there
/// too (`StepAbandoned`) and the file is FAILED `passport_missing` or
/// `passport_unreadable` — no job is ever sent without one.
pub(crate) async fn report_done<H: DriveHost>(
    cx: &mut Ops<'_>,
    meta: &FactMeta,
    file: &mut FileRow<H>,
    processing: &mut FileProcessing<H>,
) -> Result<(), DriveFault> {
    let job_id = processing.job_id;
    let next = usize::try_from(processing.step_index).unwrap_or(0) + 1;
    let has_next = file.steps.as_ref().is_some_and(|steps| next < steps.len());
    finish_job(cx, job_id)?;
    if !has_next {
        processing.report_done(meta);
        conclude::<H>(cx, meta, file).await?;
        save_processing(cx, file, processing).await?;
        crate::file::file_changed::<H>(cx, file, FileCause::ProcessingFinished)?;
        return Ok(());
    }
    if processing.cancel_requested() {
        processing.report_done(meta);
        return skip_next_step(cx, meta, file, processing, next).await;
    }
    if let Err(reason) = chain_passport(processing.passport.clone()) {
        processing.report_done(meta);
        return abandon_next_step(cx, meta, file, processing, next, reason).await;
    }
    processing.report_done_holding(
        step_number(next),
        runner_type_of(file, next),
        Uuid::now_v7(),
        meta,
    );
    save_processing(cx, file, processing).await?;
    crate::file::file_progressed::<H>(cx, file)?;
    Ok(())
}

/// Jobs ended the file's last job — reported done, the chain's next step held
/// for it — so the file is free in Jobs: the held step's `job.create` is
/// staged with the id minted at the report (a redelivered end finds the step
/// already created, and Jobs is idempotent on the id), and the file's running
/// job becomes that one (`JobCreated`, `ProcessingStarted`). A Passport no
/// longer held or readable abandons it (`StepAbandoned`). A user's cancel
/// skips a held step at once (`cancel_processing`), so none is pending here;
/// one found pending all the same skips the step (`StepSkipped`).
pub(crate) async fn release_next_step<H: DriveHost>(
    cx: &mut Ops<'_>,
    meta: &FactMeta,
    file: &mut FileRow<H>,
    processing: &mut FileProcessing<H>,
) -> Result<(), DriveFault> {
    let Some(job_id) = processing.held_job_id() else {
        return Err(DriveFault::Engine(EngineError::Config(
            "a chain's next step was released without being held".into(),
        )));
    };
    let next = usize::try_from(processing.step_index).unwrap_or(0) + 1;
    if processing.cancel_requested() {
        return skip_next_step(cx, meta, file, processing, next).await;
    }
    let passport = match chain_passport(processing.take_passport()) {
        Ok(passport) => passport,
        Err(reason) => return abandon_next_step(cx, meta, file, processing, next, reason).await,
    };
    let trigger = processing.trigger;
    let initiator = processing.triggered_by.clone();
    launch_step(
        cx, meta, file, processing, next, job_id, trigger, initiator, &passport,
    )
    .await?
    .ok_or_else(|| {
        DriveFault::Engine(EngineError::Config(
            "a chain's next step vanished under the file's lock".into(),
        ))
    })?;
    save_processing(cx, file, processing).await?;
    crate::file::file_changed::<H>(
        cx,
        file,
        FileCause::ProcessingStarted {
            job_id,
            step: step_number(next),
        },
    )?;
    Ok(())
}

/// The engine promoted the file's source and the chain's first step awaits
/// it: the step's `job.create` is staged with the id minted when the chain
/// started, and the step becomes the file's running job (`JobCreated`). A
/// Passport no longer held or readable drops the step
/// (`AwaitedStepDropped`). A user's cancel or a freeze drops an awaited step
/// at once ([`drop_awaited_step`]), so none is found cancelled here.
pub(crate) async fn release_awaited_step<H: DriveHost>(
    cx: &mut Ops<'_>,
    meta: &FactMeta,
    file: &mut FileRow<H>,
    processing: &mut FileProcessing<H>,
) -> Result<(), DriveFault> {
    let index = usize::try_from(processing.step_index).unwrap_or(0);
    let passport = match chain_passport(processing.take_passport()) {
        Ok(passport) => passport,
        Err(reason) => return drop_awaited_step(cx, meta, file, processing, reason).await,
    };
    let job_id = processing.job_id;
    let trigger = processing.trigger;
    let initiator = processing.triggered_by.clone();
    launch_step(
        cx, meta, file, processing, index, job_id, trigger, initiator, &passport,
    )
    .await?
    .ok_or_else(|| {
        DriveFault::Engine(EngineError::Config(
            "a chain's awaited first step vanished under the file's lock".into(),
        ))
    })?;
    save_processing(cx, file, processing).await?;
    crate::file::file_progressed::<H>(cx, file)?;
    Ok(())
}

/// The chain stops before its first step, which awaited the source and was
/// never asked of Jobs: the file is FAILED `reason`.
pub(crate) async fn drop_awaited_step<H: DriveHost>(
    cx: &mut Ops<'_>,
    meta: &FactMeta,
    file: &mut FileRow<H>,
    processing: &mut FileProcessing<H>,
    reason: &'static str,
) -> Result<(), DriveFault> {
    let index = usize::try_from(processing.step_index).unwrap_or(0);
    processing.drop_awaited_step(runner_type_of(file, index), reason, meta);
    save_processing(cx, file, processing).await?;
    crate::file::file_changed::<H>(
        cx,
        file,
        FileCause::ProcessingFailed {
            reason: reason.to_string(),
        },
    )?;
    Ok(())
}

/// The chain stops before step `next` on a user's cancel (or a freeze): the
/// step is skipped and the file is FAILED `cancelled`.
pub(crate) async fn skip_next_step<H: DriveHost>(
    cx: &mut Ops<'_>,
    meta: &FactMeta,
    file: &mut FileRow<H>,
    processing: &mut FileProcessing<H>,
    next: usize,
) -> Result<(), DriveFault> {
    processing.skip_step(step_number(next), runner_type_of(file, next), meta);
    save_processing(cx, file, processing).await?;
    crate::file::file_changed::<H>(
        cx,
        file,
        FileCause::ProcessingFailed {
            reason: CANCELLED.to_string(),
        },
    )?;
    Ok(())
}

/// The chain stops before step `next`: its Passport is missing or unreadable.
async fn abandon_next_step<H: DriveHost>(
    cx: &mut Ops<'_>,
    meta: &FactMeta,
    file: &mut FileRow<H>,
    processing: &mut FileProcessing<H>,
    next: usize,
    reason: &'static str,
) -> Result<(), DriveFault> {
    processing.abandon_step(step_number(next), runner_type_of(file, next), reason, meta);
    save_processing(cx, file, processing).await?;
    crate::file::file_changed::<H>(
        cx,
        file,
        FileCause::ProcessingFailed {
            reason: reason.to_string(),
        },
    )?;
    Ok(())
}

/// The chain is over: the pages numbered above the file's page count go —
/// edited ones included — and so do the images no page references any more.
/// What the run did not report again stays as it was.
async fn conclude<H: DriveHost>(
    cx: &mut Ops<'_>,
    meta: &FactMeta,
    file: &mut FileRow<H>,
) -> Result<(), DriveFault> {
    if let Some(page_count) = file.page_count {
        // A trimmed page leaves its live windows: the engine delivers the
        // `DriveRemove` by repopulation, without a cause.
        for number in store::trim_pages::<H>(cx.connection(), meta, file.id, page_count).await? {
            cx.impact::<Page>(
                &PageKey {
                    file_id: file.id,
                    number,
                },
                Dims::ALL,
            )?;
        }
    }
    let unreferenced = processed::unreferenced_images(cx.connection(), file.id).await?;
    if unreferenced.is_empty() {
        return Ok(());
    }
    for reference in drop_images(cx.connection(), file.id, &unreferenced).await? {
        cx.release_blob(BlobRef(reference))?;
    }
    file.record(
        FileEvent::ImagesDropped {
            names: unreferenced.clone(),
        },
        meta,
    );
    facts::save(cx, file).await?;
    crate::file::file_changed::<H>(
        cx,
        file,
        FileCause::ImagesDropped {
            names: unreferenced,
        },
    )?;
    Ok(())
}

/// The runner declared the file's running job failed: the job settles, the
/// file is FAILED with that reason, and Jobs is told (`job.fail`). The results
/// reported so far stay.
pub(crate) async fn report_failed<H: DriveHost>(
    cx: &mut Ops<'_>,
    meta: &FactMeta,
    file: &mut FileRow<H>,
    processing: &mut FileProcessing<H>,
    reason_code: &str,
    message: Option<&str>,
) -> Result<(), DriveFault> {
    let job_id = processing.job_id;
    processing.report_failed(reason_code, message, meta);
    let note = match message {
        Some(message) => format!("{reason_code}: {message}"),
        None => reason_code.to_string(),
    };
    cx.command(JobFail {
        payload: FailJob {
            job_id,
            note: Some(note),
        },
    })?;
    save_processing(cx, file, processing).await?;
    crate::file::file_changed::<H>(
        cx,
        file,
        FileCause::ProcessingFailed {
            reason: reason_code.to_string(),
        },
    )?;
    Ok(())
}

/// Asks Jobs to cancel the job running on the file, if any.
pub fn cancel_active_job<H: DriveHost>(
    cx: &mut Ops<'_>,
    file: &FileRow<H>,
) -> Result<(), DriveFault> {
    if let Some(job) = file.active_job() {
        cx.command(JobCancel {
            payload: CancelJob { job_id: job.job_id },
        })?;
    }
    Ok(())
}

fn finish_job(cx: &mut Ops<'_>, job_id: Uuid) -> Result<(), DriveFault> {
    cx.command(JobFinish {
        payload: FinishJob { job_id },
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ruleset(steps: Vec<RulesetStep>) -> RulesetRow {
        RulesetRow {
            id: Uuid::now_v7(),
            name: "regen".into(),
            trigger: crate::ruleset::Trigger::RegeneratePage,
            media_types: vec!["*".into()],
            steps,
            is_default: true,
            created_by: Uuid::now_v7(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn the_first_steps_options_are_merged_with_the_gestures_own() {
        let ruleset = ruleset(vec![
            RulesetStep {
                runner_type: "render".into(),
                options: serde_json::json!({ "dpi": 300 }),
            },
            RulesetStep {
                runner_type: "index".into(),
                options: serde_json::json!({}),
            },
        ]);
        let plan =
            ChainPlan::from_ruleset(&ruleset, Some(&serde_json::json!({ "page": 3, "dpi": 72 })));
        assert_eq!(plan.ruleset_id, Some(ruleset.id));
        assert_eq!(
            plan.steps[0].options,
            serde_json::json!({ "dpi": 72, "page": 3 }),
            "the gesture's keys win over the rule's"
        );
        assert_eq!(plan.steps[1].options, serde_json::json!({}));
    }

    fn human(user: Uuid, impersonator: Option<Uuid>) -> Passport {
        Passport::human(
            user,
            true,
            true,
            br_core_auth::AuthMethod::Jwt,
            impersonator,
            br_core_auth::PassportClaims::new(),
        )
    }

    #[test]
    fn a_next_step_takes_the_stored_passport_only_when_it_reads_as_one() {
        let passport = human(Uuid::now_v7(), None);
        let stored = serde_json::to_value(&passport).unwrap();
        assert_eq!(chain_passport(Some(stored)), Ok(passport));
        assert_eq!(chain_passport(None), Err(PASSPORT_MISSING));
        for unreadable in [
            serde_json::json!({ "kind": "robot" }),
            serde_json::json!("a passport"),
            serde_json::json!({ "kind": "human", "user_id": "not-a-uuid" }),
            serde_json::Value::Null,
        ] {
            assert_eq!(
                chain_passport(Some(unreadable.clone())),
                Err(PASSPORT_UNREADABLE),
                "{unreadable}"
            );
        }
    }

    #[test]
    fn jobs_is_told_the_passports_own_user_and_nobody_under_a_service_passport() {
        let user = Uuid::now_v7();
        let named = Initiator {
            id: user,
            display_name: Some("Ada".into()),
        };
        assert_eq!(
            triggered_by(&human(user, None), Some(&named)),
            Some(TriggeredBy::Identified {
                id: user,
                display_name: "Ada".into()
            })
        );
        assert_eq!(
            triggered_by(&human(user, None), None),
            Some(TriggeredBy::Anonymous(user)),
            "a chain with no initiator on record still names the passport's user"
        );
        let impersonator = Uuid::now_v7();
        assert_eq!(
            triggered_by(&human(user, Some(impersonator)), Some(&named)),
            Some(TriggeredBy::Identified {
                id: user,
                display_name: "Ada".into()
            }),
            "an impersonated user is the one acted as"
        );
        let someone_else = Initiator {
            id: Uuid::now_v7(),
            display_name: Some("Grace".into()),
        };
        assert_eq!(
            triggered_by(&human(user, None), Some(&someone_else)),
            Some(TriggeredBy::Anonymous(user)),
            "never another person's name beside the passport"
        );
        let service = Passport::service(Uuid::now_v7(), br_core_auth::PassportClaims::new());
        assert_eq!(triggered_by(&service, Some(&named)), None);
    }
}
