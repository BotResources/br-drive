use uuid::Uuid;

use super::upload::{Ticket, sha256_hex};
use super::{World, ok};

pub const RUNNER_SCOPE: &str = "workspace:runner";

pub const RENDER: &str = "render";
pub const INDEX: &str = "index";

pub struct RuleSpec<'a> {
    pub name: &'a str,
    pub trigger: &'a str,
    pub media_types: &'a [&'a str],
    pub steps: &'a [(&'a str, serde_json::Value)],
    pub is_default: bool,
}

pub async fn create_ruleset(world: &World, manager: &str, spec: RuleSpec<'_>) -> serde_json::Value {
    let steps: Vec<serde_json::Value> = spec
        .steps
        .iter()
        .map(|(runner_type, options)| {
            serde_json::json!({ "runnerType": runner_type, "options": options })
        })
        .collect();
    world
        .gql(
            manager,
            "mutation($id:UUID!,$n:String!,$t:Trigger!,$m:[String!]!,$s:[RulesetStepInput!]!,$d:Boolean!){\
             workspaceCreateRuleset(id:$id,name:$n,trigger:$t,mediaTypes:$m,steps:$s,isDefault:$d){id unknownRunnerTypes}}",
            serde_json::json!({
                "id": Uuid::now_v7(),
                "n": spec.name,
                "t": spec.trigger,
                "m": spec.media_types,
                "s": steps,
                "d": spec.is_default,
            }),
        )
        .await
}

pub fn ruleset_id(response: &serde_json::Value) -> Uuid {
    Uuid::parse_str(
        ok(response)["workspaceCreateRuleset"]["id"]
            .as_str()
            .expect("the ruleset id"),
    )
    .expect("a uuid")
}

/// Declares the two stand-in runner types ACTIVE and one default upload rule
/// `text/plain → [render]`, so an uploaded text file gets one job.
pub async fn install_render_rule(world: &World, jobs: &super::JobsStandIn, manager: &str) -> Uuid {
    jobs.declare_runner_type(RENDER, contract_jobs::catalog::RunnerTypeLifecycle::Active)
        .await;
    jobs.declare_runner_type(INDEX, contract_jobs::catalog::RunnerTypeLifecycle::Active)
        .await;
    world.await_known_runner_type(RENDER, Some("active")).await;
    world.await_known_runner_type(INDEX, Some("active")).await;
    ruleset_id(
        &create_ruleset(
            world,
            manager,
            RuleSpec {
                name: "render text",
                trigger: "UPLOAD",
                media_types: &["text/plain"],
                steps: &[(RENDER, serde_json::json!({}))],
                is_default: true,
            },
        )
        .await,
    )
}

pub async fn install_regenerate_rule(world: &World, manager: &str) -> Uuid {
    ruleset_id(
        &create_ruleset(
            world,
            manager,
            RuleSpec {
                name: "regenerate text page",
                trigger: "REGENERATE_PAGE",
                media_types: &["text/plain"],
                steps: &[(RENDER, serde_json::json!({}))],
                is_default: true,
            },
        )
        .await,
    )
}

/// Drives one job to completion the way the runner and jobs would: the runner
/// reports `done`, the host finishes the job, jobs confirms it completed.
pub async fn finish_job(world: &World, jobs: &super::JobsStandIn, job_id: Uuid) {
    jobs.await_finish(job_id).await;
    jobs.complete(job_id).await;
    let _ = world;
}

pub async fn context(
    world: &World,
    runner: &str,
    file_id: Uuid,
    job_id: Uuid,
) -> serde_json::Value {
    world
        .gql(
            runner,
            "query($f:UUID!,$j:UUID!){workspaceRunnerContext(fileId:$f,jobId:$j){fileId mediaType \
             name pageCount summary sourceUrl images pages{number markdown origin}}}",
            serde_json::json!({ "f": file_id, "j": job_id }),
        )
        .await
}

pub async fn upload_image(
    world: &World,
    runner: &str,
    file_id: Uuid,
    job_id: Uuid,
    name: &str,
    bytes: &[u8],
) -> String {
    let ticket = image_ticket(&request_image(world, runner, file_id, job_id, name, bytes).await);
    let status = super::upload::post_bytes(world, &ticket, bytes, name).await;
    assert!((200..300).contains(&status), "{name} lands: {status}");
    ticket.fields["key"].as_str().unwrap().to_string()
}

pub async fn request_image(
    world: &World,
    runner: &str,
    file_id: Uuid,
    job_id: Uuid,
    name: &str,
    bytes: &[u8],
) -> serde_json::Value {
    world
        .gql(
            runner,
            "mutation($f:UUID!,$j:UUID!,$n:String!,$m:String!,$s:ByteCount!,$h:String!){\
             workspaceRunnerRequestImageUpload(fileId:$f,jobId:$j,name:$n,mediaType:$m,size:$s,sha256:$h)\
             {fileId url fields}}",
            serde_json::json!({
                "f": file_id,
                "j": job_id,
                "n": name,
                "m": "image/png",
                "s": bytes.len(),
                "h": sha256_hex(bytes),
            }),
        )
        .await
}

pub fn image_ticket(response: &serde_json::Value) -> Ticket {
    image_ticket_of(response, "workspaceRunnerRequestImageUpload")
}

/// The upload ticket a mutation root answered, whichever root it is.
pub fn image_ticket_of(response: &serde_json::Value, root: &str) -> Ticket {
    let post = &ok(response)[root];
    Ticket {
        file_id: Uuid::parse_str(post["fileId"].as_str().expect("the file id")).expect("a uuid"),
        url: post["url"]
            .as_str()
            .expect("a presigned endpoint")
            .to_string(),
        fields: post["fields"]
            .as_object()
            .expect("presigned post fields")
            .clone(),
    }
}

pub struct Report<'a> {
    pub job_id: Uuid,
    pub pages: Vec<(i32, &'a str)>,
    pub origin: Option<&'a str>,
    pub indexer: Option<(&'a str, i32, i64)>,
    pub done: bool,
}

pub async fn report(
    world: &World,
    runner: &str,
    file_id: Uuid,
    report: Report<'_>,
) -> serde_json::Value {
    let pages: Vec<serde_json::Value> = report
        .pages
        .iter()
        .map(|(number, markdown)| serde_json::json!({ "number": number, "markdown": markdown }))
        .collect();
    let mut variables = serde_json::json!({
        "f": file_id,
        "j": report.job_id,
        "p": pages,
        "d": report.done,
    });
    if let Some(origin) = report.origin {
        variables["o"] = serde_json::json!(origin);
    }
    if let Some((summary, page_count, estimated_tokens)) = report.indexer {
        variables["s"] = serde_json::json!(summary);
        variables["c"] = serde_json::json!(page_count);
        variables["t"] = serde_json::json!(estimated_tokens);
    }
    world
        .gql(
            runner,
            "mutation($f:UUID!,$j:UUID!,$p:[ReportedPageInput!]!,$o:PageOrigin,$s:String,$c:Int,$t:Int,$d:Boolean!){\
             workspaceRunnerReport(fileId:$f,jobId:$j,pages:$p,origin:$o,summary:$s,pageCount:$c,estimatedTokens:$t,done:$d){success}}",
            variables,
        )
        .await
}
