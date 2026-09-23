use super::pages::{DrivePage, DrivePages};
use super::view::{DriveFile, DriveFiles};
use crate::host::DriveHost;
use crate::label::{DriveLabel, DriveLabels};
use crate::ruleset::{DriveRuleset, DriveRulesets};

service_engine::subscription_union! {
    generics [ H: DriveHost ];
    view = DriveView;
    delta = DriveDelta { reset = DriveReset, upsert = DriveUpsert, remove = DriveRemove };
    File => ::service_engine::view::ViewProjector<DriveFiles<H>> => DriveFile,
    Page => ::service_engine::view::ViewProjector<DrivePages<H>> => DrivePage,
    Label => ::service_engine::view::ViewProjector<DriveLabels<H>> => DriveLabel,
    Ruleset => ::service_engine::view::ViewProjector<DriveRulesets<H>> => DriveRuleset,
}
