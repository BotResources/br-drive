use std::any::TypeId;
use std::collections::HashMap;
use std::sync::Mutex;

use service_engine::error::EngineError;
use service_engine::graphql::RootPrefix;

use crate::host::DriveHost;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootNames {
    pub context_root: String,
    pub image_upload_root: String,
    pub report_root: String,
}

impl RootNames {
    pub fn for_prefix(prefix: &'static str) -> Result<Self, EngineError> {
        let prefix = RootPrefix::from_snake(prefix)?;
        let camel = prefix.as_str();
        Ok(Self {
            context_root: format!("{camel}RunnerContext"),
            image_upload_root: format!("{camel}RunnerRequestImageUpload"),
            report_root: format!("{camel}RunnerReport"),
        })
    }
}

static ROOTS: Mutex<Option<HashMap<TypeId, RootNames>>> = Mutex::new(None);

pub fn declare_roots<H: DriveHost>(prefix: &'static str) -> Result<(), EngineError> {
    let roots = RootNames::for_prefix(prefix)?;
    let mut declared = ROOTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let declared = declared.get_or_insert_with(HashMap::new);
    match declared.get(&TypeId::of::<H>()) {
        Some(existing) if *existing == roots => Ok(()),
        Some(existing) => Err(EngineError::Config(format!(
            "the drive slice is already registered for this principal under the root names \
             {existing:?}; one drive slice per host principal"
        ))),
        None => {
            declared.insert(TypeId::of::<H>(), roots);
            Ok(())
        }
    }
}

pub(super) fn roots<H: DriveHost>() -> Result<RootNames, EngineError> {
    ROOTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .as_ref()
        .and_then(|declared| declared.get(&TypeId::of::<H>()).cloned())
        .ok_or_else(|| {
            EngineError::Config(
                "the drive slice's root names are not declared for this principal; register the \
                 slice through `drive_slice!` before starting a processing chain"
                    .into(),
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_root_names_are_camel_cased_like_the_graphql_fields() {
        let roots = RootNames::for_prefix("workspace").unwrap();
        assert_eq!(roots.context_root, "workspaceRunnerContext");
        let roots = RootNames::for_prefix("my_host").unwrap();
        assert_eq!(roots.context_root, "myHostRunnerContext");
        assert_eq!(roots.image_upload_root, "myHostRunnerRequestImageUpload");
        assert_eq!(roots.report_root, "myHostRunnerReport");
        assert!(RootNames::for_prefix("Bad_Prefix").is_err());
    }
}
