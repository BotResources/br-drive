use super::view::{DriveFile, DriveFiles};
use crate::host::DriveHost;

service_engine::subscription_union! {
    generics [ H: DriveHost ];
    view = DriveFileUnion;
    delta = DriveDelta { reset = DriveReset, upsert = DriveUpsert, remove = DriveRemove };
    File => ::service_engine::view::ViewProjector<DriveFiles<H>> => DriveFile,
}
