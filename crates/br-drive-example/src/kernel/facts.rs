use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use uuid::Uuid;

pub struct OwnedWorkspaces(pub Vec<Uuid>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HostSettings {
    pub upload_window: Duration,
}

impl HostSettings {
    pub const DEFAULT_UPLOAD_WINDOW: Duration = Duration::from_secs(15 * 60);
}

impl Default for HostSettings {
    fn default() -> Self {
        Self {
            upload_window: Self::DEFAULT_UPLOAD_WINDOW,
        }
    }
}

#[derive(Default, Clone)]
pub struct PrincipalFacts {
    map: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

impl PrincipalFacts {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert<F: Any + Send + Sync>(&mut self, fact: F) {
        self.map.insert(TypeId::of::<F>(), Arc::new(fact));
    }

    pub fn get<F: Any + Send + Sync>(&self) -> Option<&F> {
        self.map
            .get(&TypeId::of::<F>())
            .and_then(|fact| fact.downcast_ref::<F>())
    }
}

impl std::fmt::Debug for PrincipalFacts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrincipalFacts")
            .field("facts", &self.map.len())
            .finish()
    }
}
