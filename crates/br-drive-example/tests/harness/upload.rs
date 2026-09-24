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
