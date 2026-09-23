use futures_util::future::BoxFuture;
use service_engine::PostUpload;
use service_engine::blobs::Uploaded;
use service_engine::pipeline::PostSave;
use service_engine::{Blobs, Engine};
use sqlx::Row;
use uuid::Uuid;

use crate::file::{File, FileCause};
use crate::host::DriveHost;

pub struct DriveSource;

impl Blobs for DriveSource {
    const KIND: &'static str = "drive_source";
}

pub struct DriveImage;

impl Blobs for DriveImage {
    const KIND: &'static str = "drive_image";
}

pub fn image_available<'a>(
    uploaded: Uploaded<'a>,
    ps: &'a mut PostSave<'_, '_>,
) -> BoxFuture<'a, Result<(), PostUpload>> {
    Box::pin(async move {
        let found =
            crate::file::store::file_of_image(ps.connection(), uploaded.reference.as_uuid())
                .await?;
        if let Some((file_id, name)) = found {
            ps.impact_caused::<File, _>(&file_id, FileCause::ImageAvailable { name })?;
        }
        Ok(())
    })
}

pub fn source_available<'a>(
    uploaded: Uploaded<'a>,
    ps: &'a mut PostSave<'_, '_>,
) -> BoxFuture<'a, Result<(), PostUpload>> {
    Box::pin(async move {
        let row = sqlx::query("SELECT id FROM drive.file WHERE blob_ref = $1")
            .bind(uploaded.reference.as_uuid())
            .fetch_optional(ps.connection())
            .await?;
        if let Some(row) = row {
            let id: Uuid = row.get("id");
            ps.impact_caused::<File, _>(&id, FileCause::SourceAvailable)?;
        }
        Ok(())
    })
}

pub fn register<P: DriveHost>(
    engine: &mut Engine<P>,
) -> Result<(), service_engine::error::EngineError> {
    engine.register_blobs::<DriveSource>(service_engine::BlobPolicy {
        max_bytes: P::SOURCE_MAX_BYTES,
        orphan_after: P::SOURCE_ORPHAN_AFTER,
    })?;
    engine.register_post_upload_policy::<DriveSource, _>(source_available)?;
    engine.require_post_upload_policy::<DriveSource>()?;
    engine.register_blobs::<DriveImage>(service_engine::BlobPolicy {
        max_bytes: P::IMAGE_MAX_BYTES,
        orphan_after: P::IMAGE_ORPHAN_AFTER,
    })?;
    engine.register_post_upload_policy::<DriveImage, _>(image_available)?;
    engine.require_post_upload_policy::<DriveImage>()?;
    Ok(())
}
