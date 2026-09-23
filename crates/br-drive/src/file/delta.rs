use super::pages::{DrivePage, DrivePages};
use super::view::{DriveFile, DriveFiles};
use crate::host::DriveHost;

service_engine::subscription_union! {
    generics [ H: DriveHost ];
    view = DriveView;
    delta = DriveDelta { reset = DriveReset, upsert = DriveUpsert, remove = DriveRemove };
    File => ::service_engine::view::ViewProjector<DriveFiles<H>> => DriveFile,
    Page => ::service_engine::view::ViewProjector<DrivePages<H>> => DrivePage,
}
