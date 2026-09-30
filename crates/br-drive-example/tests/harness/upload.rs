use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{World, error_code, ok};

pub fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub struct Ticket {
    pub file_id: Uuid,
    pub url: String,
    pub fields: serde_json::Map<String, serde_json::Value>,
}

pub struct UploadRequest<'a> {
    pub drive: Uuid,
    pub path: &'a str,
    pub name: &'a str,
    pub media_type: &'a str,
    pub bytes: &'a [u8],
    pub title: Option<&'a str>,
}

impl<'a> UploadRequest<'a> {
    pub fn text(drive: Uuid, path: &'a str, name: &'a str, bytes: &'a [u8]) -> Self {
        Self {
            drive,
            path,
            name,
            media_type: "text/plain",
            bytes,
            title: None,
        }
    }
}

const REQUEST: &str = "mutation($f:UUID!,$d:UUID!,$p:String!,$n:String!,$m:String!,$s:ByteCount!,$h:String!,$t:String){\
    workspaceRequestUpload(fileId:$f,driveId:$d,path:$p,name:$n,mediaType:$m,size:$s,sha256:$h,title:$t){fileId url fields}}";

pub async fn request(
    world: &World,
    passport: &str,
    file_id: Uuid,
    request: &UploadRequest<'_>,
) -> serde_json::Value {
    request_with_hash(
        world,
        passport,
        file_id,
        request,
        &sha256_hex(request.bytes),
    )
    .await
}

pub async fn request_with_hash(
    world: &World,
    passport: &str,
    file_id: Uuid,
    request: &UploadRequest<'_>,
    sha256: &str,
) -> serde_json::Value {
    world
        .gql(
            passport,
            REQUEST,
            serde_json::json!({
                "f": file_id,
                "d": request.drive,
                "p": request.path,
                "n": request.name,
                "m": request.media_type,
                "s": request.bytes.len(),
                "h": sha256,
                "t": request.title,
            }),
        )
        .await
}

const REQUEST_WITH_RULESET: &str = "mutation($f:UUID!,$d:UUID!,$p:String!,$n:String!,$m:String!,$s:ByteCount!,$h:String!,$r:UUID){\
    workspaceRequestUpload(fileId:$f,driveId:$d,path:$p,name:$n,mediaType:$m,size:$s,sha256:$h,rulesetId:$r){fileId url fields}}";

/// `RequestUpload` naming the `upload` rule its commit runs.
pub async fn request_with_ruleset(
    world: &World,
    passport: &str,
    file_id: Uuid,
    request: &UploadRequest<'_>,
    ruleset: Uuid,
) -> serde_json::Value {
    world
        .gql(
            passport,
            REQUEST_WITH_RULESET,
            serde_json::json!({
                "f": file_id,
                "d": request.drive,
                "p": request.path,
                "n": request.name,
                "m": request.media_type,
                "s": request.bytes.len(),
                "h": sha256_hex(request.bytes),
                "r": ruleset,
            }),
        )
        .await
}

pub fn ticket(response: &serde_json::Value) -> Ticket {
    let post = &ok(response)["workspaceRequestUpload"];
    Ticket {
        file_id: Uuid::parse_str(post["fileId"].as_str().expect("the file id"))
            .expect("a uuid file id"),
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

pub async fn post_bytes(world: &World, ticket: &Ticket, bytes: &[u8], file_name: &str) -> u16 {
    let mut form = reqwest::multipart::Form::new();
    for (name, value) in &ticket.fields {
        form = form.text(name.clone(), value.as_str().unwrap().to_string());
    }
    form = form.part(
        "file",
        reqwest::multipart::Part::bytes(bytes.to_vec()).file_name(file_name.to_string()),
    );
    world
        .http
        .post(&ticket.url)
        .multipart(form)
        .send()
        .await
        .expect("the presigned upload reaches object storage")
        .status()
        .as_u16()
}

pub async fn commit(world: &World, passport: &str, file_id: Uuid) -> serde_json::Value {
    world
        .gql(
            passport,
            "mutation($f:UUID!){workspaceCommitUpload(fileId:$f){success}}",
            serde_json::json!({ "f": file_id }),
        )
        .await
}

pub async fn upload(world: &World, passport: &str, request: &UploadRequest<'_>) -> Uuid {
    let file_id = Uuid::now_v7();
    let ticket = ticket(&self::request(world, passport, file_id, request).await);
    let status = post_bytes(world, &ticket, request.bytes, request.name).await;
    assert!(
        (200..300).contains(&status),
        "object storage accepts the exact bytes: {status}"
    );
    let committed = commit(world, passport, file_id).await;
    assert!(
        committed.get("errors").is_none(),
        "the commit is acked: {} ({})",
        committed,
        error_code(&committed)
    );
    file_id
}

pub const PROCESS_FILE: &str =
    "mutation($f:UUID!,$r:UUID){workspaceProcessFile(fileId:$f,rulesetId:$r){success}}";

/// The user's `ProcessFile` gesture, with the default rule of its trigger.
pub async fn process(world: &World, passport: &str, file_id: Uuid) -> serde_json::Value {
    process_with(world, passport, file_id, None).await
}

/// The user's `ProcessFile` gesture with a rule of their choosing.
pub async fn process_with(
    world: &World,
    passport: &str,
    file_id: Uuid,
    ruleset: Option<Uuid>,
) -> serde_json::Value {
    world
        .gql(
            passport,
            PROCESS_FILE,
            serde_json::json!({ "f": file_id, "r": ruleset }),
        )
        .await
}

/// An upload committed, then processed: what a front does right after the
/// commit when its user wants the file processed.
pub async fn upload_processed(world: &World, passport: &str, request: &UploadRequest<'_>) -> Uuid {
    let file_id = upload(world, passport, request).await;
    let processed = process(world, passport, file_id).await;
    assert!(
        processed.get("errors").is_none(),
        "the process gesture is acked: {} ({})",
        processed,
        error_code(&processed)
    );
    file_id
}
