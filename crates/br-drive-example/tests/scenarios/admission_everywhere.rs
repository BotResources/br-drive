//! The host's caller admission on every root: `DriveHost::admit` is asked
//! first by every query, mutation and subscription of the drive slice, before
//! any lookup. A caller the host does not admit — here a deactivated account,
//! refused `ACTIVE_USER_REQUIRED` — gets the host's code at the root, the same
//! bytes whether the id it named exists or not, and changes nothing. The
//! runner roots keep their own runner-scope check, asked before any lookup.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use async_graphql::Schema;
use br_drive_example::slices::drive::{DriveMutation, DriveQuery, DriveSubscription};
use uuid::Uuid;

use crate::harness::runner::{RENDER, RuleSpec, create_ruleset, ruleset_id};
use crate::harness::upload::{UploadRequest, upload};
use crate::harness::{
    DRIVE_DELTAS, LABEL_DELTAS, PAGE_DELTAS, RULESET_DELTAS, SseSubscription, Subscription, World,
    WorldOptions, deactivated_passport_with_scopes, error_code, manager_passport_with_scopes, ok,
};

const REFUSED: &str = "ACTIVE_USER_REQUIRED";
const RUNNER_REFUSED: &str = "RUNNER_SCOPE_REQUIRED";
const MANAGE: &str = "workspace:manage";
const BYTES: &[u8] = b"admission everywhere";
const WAIT: Duration = Duration::from_secs(10);

/// The ids a root may name: an existing set and a random one.
#[derive(Clone, Copy)]
struct Ids {
    drive: Uuid,
    file: Uuid,
    label: Uuid,
    ruleset: Uuid,
    job: Uuid,
}

impl Ids {
    fn random() -> Self {
        Self {
            drive: Uuid::now_v7(),
            file: Uuid::now_v7(),
            label: Uuid::now_v7(),
            ruleset: Uuid::now_v7(),
            job: Uuid::now_v7(),
        }
    }

    /// The variables `query` declares, bound to these ids.
    fn variables(&self, query: &str) -> serde_json::Value {
        let mut variables = serde_json::Map::new();
        for (name, id) in [
            ("d", self.drive),
            ("f", self.file),
            ("l", self.label),
            ("r", self.ruleset),
            ("j", self.job),
        ] {
            if query.contains(&format!("${name}:")) {
                variables.insert(name.to_string(), serde_json::json!(id));
            }
        }
        serde_json::Value::Object(variables)
    }
}

/// Every admitted query and mutation root of the drive slice, as (root, document).
const ROOTS: &[(&str, &str)] = &[
    (
        "workspaceFile",
        "query($f:UUID!){workspaceFile(fileId:$f){id}}",
    ),
    (
        "workspaceDriveFiles",
        "query($d:UUID!){workspaceDriveFiles(driveId:$d){id}}",
    ),
    (
        "workspacePages",
        "query($f:UUID!){workspacePages(fileId:$f){number}}",
    ),
    ("workspaceRulesets", "query{workspaceRulesets{id}}"),
    (
        "workspaceRunnerTypes",
        "query{workspaceRunnerTypes{runnerType}}",
    ),
    ("workspaceLabels", "query{workspaceLabels{id}}"),
    (
        "workspaceFileAccess",
        "query($f:UUID!){workspaceFileAccess(fileId:$f)}",
    ),
    (
        "workspaceRequestUpload",
        "mutation($f:UUID!,$d:UUID!){workspaceRequestUpload(fileId:$f,driveId:$d,path:\"\",\
         name:\"late.txt\",mediaType:\"text/plain\",size:3,\
         sha256:\"0000000000000000000000000000000000000000000000000000000000000000\"){fileId}}",
    ),
    (
        "workspaceCommitUpload",
        "mutation($f:UUID!){workspaceCommitUpload(fileId:$f){success}}",
    ),
    (
        "workspaceProcessFile",
        "mutation($f:UUID!){workspaceProcessFile(fileId:$f){success}}",
    ),
    (
        "workspaceCancelProcessing",
        "mutation($f:UUID!){workspaceCancelProcessing(fileId:$f){success}}",
    ),
    (
        "workspaceRegeneratePage",
        "mutation($f:UUID!){workspaceRegeneratePage(fileId:$f,number:1){success}}",
    ),
    (
        "workspaceCreateRuleset",
        "mutation($r:UUID!){workspaceCreateRuleset(id:$r,name:\"late\",trigger:UPLOAD,\
         mediaTypes:[\"text/plain\"],steps:[]){id}}",
    ),
    (
        "workspaceUpdateRuleset",
        "mutation($r:UUID!){workspaceUpdateRuleset(id:$r,name:\"renamed\"){id}}",
    ),
    (
        "workspaceCreateLabel",
        "mutation($l:UUID!){workspaceCreateLabel(id:$l,name:\"late\",color:\"#112233\"){success}}",
    ),
    (
        "workspaceUpdateLabel",
        "mutation($l:UUID!){workspaceUpdateLabel(id:$l,name:\"renamed\"){success}}",
    ),
    (
        "workspaceDeleteLabel",
        "mutation($l:UUID!){workspaceDeleteLabel(id:$l){success}}",
    ),
    (
        "workspaceSetFileLabels",
        "mutation($f:UUID!,$l:UUID!){workspaceSetFileLabels(fileId:$f,labelIds:[$l]){success}}",
    ),
    (
        "workspaceDeleteRuleset",
        "mutation($r:UUID!){workspaceDeleteRuleset(id:$r){success}}",
    ),
    (
        "workspaceUpdateFile",
        "mutation($f:UUID!){workspaceUpdateFile(fileId:$f,name:\"renamed.txt\"){success}}",
    ),
    (
        "workspaceRetitleFile",
        "mutation($f:UUID!){workspaceRetitleFile(fileId:$f,title:\"renamed\"){success}}",
    ),
    (
        "workspaceDeleteFile",
        "mutation($f:UUID!){workspaceDeleteFile(fileId:$f){success}}",
    ),
    (
        "workspaceEditPage",
        "mutation($f:UUID!){workspaceEditPage(fileId:$f,number:1,markdown:\"x\"){success}}",
    ),
    (
        "workspaceMoveFolder",
        "mutation($d:UUID!){workspaceMoveFolder(driveId:$d,oldPrefix:\"a/\",newPrefix:\"b/\"){success}}",
    ),
    (
        "workspaceDeleteFolder",
        "mutation($d:UUID!){workspaceDeleteFolder(driveId:$d,prefix:\"a/\"){success}}",
    ),
];

/// The runner roots: the runner scope, not the host's admission, answers there.
const RUNNER_ROOTS: &[(&str, &str)] = &[
    (
        "workspaceRunnerContext",
        "query($f:UUID!,$j:UUID!){workspaceRunnerContext(fileId:$f,jobId:$j){fileId}}",
    ),
    (
        "workspaceRunnerRequestImageUpload",
        "mutation($f:UUID!,$j:UUID!){workspaceRunnerRequestImageUpload(fileId:$f,jobId:$j,\
         name:\"p1.png\",mediaType:\"image/png\",size:3,\
         sha256:\"0000000000000000000000000000000000000000000000000000000000000000\"){fileId}}",
    ),
    (
        "workspaceRunnerReport",
        "mutation($f:UUID!,$j:UUID!){workspaceRunnerReport(fileId:$f,jobId:$j,done:true){success}}",
    ),
    (
        "workspaceRunnerReportFailure",
        "mutation($f:UUID!,$j:UUID!){workspaceRunnerReportFailure(fileId:$f,jobId:$j,\
         reasonCode:\"BROKEN\"){success}}",
    ),
];

/// The four subscriptions, as (root, document).
const SUBSCRIPTIONS: &[(&str, &str)] = &[
    ("workspaceDriveChanged", DRIVE_DELTAS),
    ("workspaceFilePages", PAGE_DELTAS),
    ("workspaceLabelsChanged", LABEL_DELTAS),
    ("workspaceRulesetsChanged", RULESET_DELTAS),
];

/// Every root the drive slice declares, read from its schema.
fn declared_roots() -> BTreeSet<String> {
    let sdl = Schema::build(DriveQuery, DriveMutation, DriveSubscription)
        .finish()
        .sdl();
    let mut roots = BTreeSet::new();
    let mut inside = false;
    for line in sdl.lines() {
        if [
            "type DriveQuery",
            "type DriveMutation",
            "type DriveSubscription",
        ]
        .iter()
        .any(|head| line.starts_with(head))
        {
            inside = true;
        } else if line.starts_with('}') {
            inside = false;
        } else if inside {
            let name = line.trim().split(['(', ':']).next().unwrap_or_default();
            if !name.is_empty() && !name.starts_with('"') {
                roots.insert(name.to_string());
            }
        }
    }
    roots
}

#[test]
fn the_suite_covers_every_root_of_the_drive_slice() {
    let covered: BTreeSet<String> = ROOTS
        .iter()
        .chain(RUNNER_ROOTS)
        .chain(SUBSCRIPTIONS)
        .map(|(root, _)| (*root).to_string())
        .collect();
    assert_eq!(
        declared_roots(),
        covered,
        "every root of the drive slice is proven refused to a caller the host does not admit"
    );
}

/// The engine's liveness rows, rewritten on their own clock whatever the
/// callers do.
const HEARTBEATS: &[&str] = &[
    "service_engine.leader_slot",
    "service_engine.schema_version",
];

/// What the database holds, table by table: a digest of every row of every
/// table — the library's, the host's and the engine's (blobs, outbox, logs) —
/// but the engine's heartbeats.
async fn fingerprint(world: &World) -> BTreeMap<String, String> {
    let owner = world.db.owner().await;
    let tables: Vec<(String, String)> = sqlx::query_as(
        "SELECT table_schema::text, table_name::text FROM information_schema.tables \
         WHERE table_type = 'BASE TABLE' \
           AND table_schema NOT IN ('pg_catalog', 'information_schema') \
         ORDER BY 1, 2",
    )
    .fetch_all(&owner)
    .await
    .expect("list the tables");
    let mut digests = BTreeMap::new();
    for (schema, table) in tables {
        let digest: Option<String> = sqlx::query_scalar(&format!(
            "SELECT md5(coalesce(string_agg(t::text, '|' ORDER BY t::text), '')) \
             FROM \"{schema}\".\"{table}\" t"
        ))
        .fetch_one(&owner)
        .await
        .expect("digest a table");
        let name = format!("{schema}.{table}");
        if !HEARTBEATS.contains(&name.as_str()) {
            digests.insert(name, digest.unwrap_or_default());
        }
    }
    owner.close().await;
    digests
}

/// A person's workspace holding one file, one label and one ruleset, seen
/// by the person while admitted.
async fn existing(world: &World, admitted: &str) -> Ids {
    let drive = world.create_workspace(admitted, "admission").await;
    let file = upload(
        world,
        admitted,
        &UploadRequest::text(drive, "a/", "kept.txt", BYTES),
    )
    .await;
    world.await_source_promoted(file).await;
    let label = Uuid::now_v7();
    ok(&world
        .gql(
            admitted,
            "mutation($l:UUID!){workspaceCreateLabel(id:$l,name:\"kept\",color:\"#445566\"){success}}",
            serde_json::json!({ "l": label }),
        )
        .await);
    let ruleset = ruleset_id(
        &create_ruleset(
            world,
            admitted,
            RuleSpec {
                name: "kept",
                trigger: "UPLOAD",
                media_types: &["text/plain"],
                steps: &[(RENDER, serde_json::json!({}))],
                is_default: false,
            },
        )
        .await,
    );
    Ids {
        drive,
        file,
        label,
        ruleset,
        job: Uuid::now_v7(),
    }
}

#[tokio::test]
async fn a_caller_the_host_does_not_admit_is_refused_at_every_root_whether_the_id_exists_or_not() {
    // Given: a person's workspace with a file, a label and a ruleset, then the
    // person's account deactivated
    let world = World::start_with(
        "pod-admission-everywhere",
        WorldOptions {
            watch_catalogue: false,
            ..WorldOptions::default()
        },
    )
    .await;
    let person = Uuid::now_v7();
    let admitted = manager_passport_with_scopes(person, "Ada", &[MANAGE]);
    let refused = deactivated_passport_with_scopes(person, &[MANAGE]);
    let known = existing(&world, &admitted).await;
    let unknown = Ids::random();
    let before = fingerprint(&world).await;

    // When: the deactivated person calls every query and mutation root
    for (root, document) in ROOTS {
        let on_known = world
            .gql(&refused, document, known.variables(document))
            .await;
        let on_unknown = world
            .gql(&refused, document, unknown.variables(document))
            .await;

        // Then: the host's code, at the root
        assert_eq!(error_code(&on_known), REFUSED, "{root}: {on_known}");
        assert_eq!(
            on_known["errors"][0]["path"][0], *root,
            "{root} answers at the root: {on_known}"
        );
        // And: the same bytes whether the id exists or not
        assert_eq!(
            on_known.to_string(),
            on_unknown.to_string(),
            "{root} tells nothing of the existence of what it names"
        );
    }

    // When: the deactivated person opens every subscription, over both transports
    for (root, document) in SUBSCRIPTIONS {
        let mut ws_errors = Vec::new();
        let mut sse_errors = Vec::new();
        for ids in [known, unknown] {
            let mut sub = Subscription::open_with(
                &world.subscription_url(),
                &refused,
                document,
                ids.variables(document),
            )
            .await;
            let errors = sub.next_error(WAIT).await;
            // And: no stream is opened
            sub.expect_silence(Duration::from_millis(400)).await;
            ws_errors.push(errors);
            let sse = SseSubscription::open(
                &world.http,
                &world.service.http("/graphql"),
                &refused,
                document,
                ids.variables(document),
            )
            .await;
            sse_errors.push(sse.refusal(WAIT).await);
        }
        // Then: the host's code, the same bytes for an existing and a random id
        for errors in [&ws_errors, &sse_errors] {
            assert_eq!(
                errors[0][0]["extensions"]["code"], REFUSED,
                "{root}: {errors:?}"
            );
            assert_eq!(
                errors[0].to_string(),
                errors[1].to_string(),
                "{root} tells nothing of the existence of what it names"
            );
        }
    }

    // Then: nothing was recorded, changed or published
    let after = fingerprint(&world).await;
    let changed: BTreeSet<&String> = before
        .keys()
        .chain(after.keys())
        .filter(|table| before.get(*table) != after.get(*table))
        .collect();
    assert!(changed.is_empty(), "refused calls changed {changed:?}");

    // And: the same person, admitted again, reads as before
    let file = world.file(&admitted, known.file).await;
    assert_eq!(file["name"], "kept.txt");
    assert_eq!(file["processingState"], "READY");
    assert_eq!(world.drive_files(&admitted, known.drive).await.len(), 1);
    assert_eq!(world.labels(&admitted).await.len(), 1);
    assert_eq!(world.rulesets(&admitted).await.len(), 1);

    world.cleanup().await;
}

#[tokio::test]
async fn the_runner_roots_keep_their_runner_scope_for_admitted_and_refused_callers_alike() {
    // Given: a person's workspace with a file
    let world = World::start("pod-admission-runner-roots").await;
    let person = Uuid::now_v7();
    let admitted = manager_passport_with_scopes(person, "Ada", &[MANAGE]);
    let refused = deactivated_passport_with_scopes(person, &[MANAGE]);
    let known = existing(&world, &admitted).await;
    let unknown = Ids::random();

    for (root, document) in RUNNER_ROOTS {
        for caller in [&admitted, &refused] {
            // When: a person, admitted or not, calls a runner root
            let on_known = world.gql(caller, document, known.variables(document)).await;
            let on_unknown = world
                .gql(caller, document, unknown.variables(document))
                .await;
            // Then: the runner scope is required, before any lookup
            assert_eq!(error_code(&on_known), RUNNER_REFUSED, "{root}: {on_known}");
            assert_eq!(
                on_known.to_string(),
                on_unknown.to_string(),
                "{root} tells nothing of the existence of what it names"
            );
        }
    }

    world.cleanup().await;
}
