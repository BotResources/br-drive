use std::any::Any;

use br_core_auth::{Passport, PassportClaims};
use br_core_integration::Actor;
use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::principal::{Principal, PrincipalId, PrincipalResolver};
use service_engine::{PassportPrincipal, PrincipalRejected};
use sqlx::PgPool;
use uuid::Uuid;

use crate::kernel::facts::{OwnedWorkspaces, PrincipalFacts};

#[derive(Debug, Clone)]
pub struct AppPrincipal {
    id: PrincipalId,
    passport: Passport,
    facts: PrincipalFacts,
}

impl AppPrincipal {
    pub fn new(user: Uuid) -> Self {
        Self {
            id: PrincipalId::from(user),
            passport: Passport::human(
                user,
                false,
                true,
                br_core_auth::AuthMethod::Jwt,
                None,
                PassportClaims::new(),
            ),
            facts: PrincipalFacts::new(),
        }
    }

    pub fn from_actor(actor: Actor) -> Self {
        let passport = match actor {
            Actor::Service(id) => Passport::service(id.as_uuid(), PassportClaims::new()),
            Actor::Human(id) => Passport::human(
                id.as_uuid(),
                false,
                true,
                br_core_auth::AuthMethod::Jwt,
                None,
                PassportClaims::new(),
            ),
        };
        Self {
            id: PrincipalId::from(actor.id()),
            passport,
            facts: PrincipalFacts::new(),
        }
    }

    pub fn with_fact<F: Any + Send + Sync>(mut self, fact: F) -> Self {
        self.facts.insert(fact);
        self
    }

    pub fn user(&self) -> Uuid {
        self.id.as_uuid()
    }

    pub fn is_service(&self) -> bool {
        self.passport.service_account_id().is_some()
    }

    pub fn holds_scope(&self, scope: &str) -> bool {
        self.passport
            .claim::<Vec<String>>(br_drive::SCOPES_CLAIM)
            .is_some_and(|scopes| scopes.iter().any(|held| held == scope))
    }

    pub fn facts(&self) -> &PrincipalFacts {
        &self.facts
    }

    pub fn facts_mut(&mut self) -> &mut PrincipalFacts {
        &mut self.facts
    }

    pub fn owns_workspace(&self, workspace: Uuid) -> bool {
        self.facts()
            .get::<OwnedWorkspaces>()
            .is_some_and(|OwnedWorkspaces(owned)| owned.contains(&workspace))
    }

    pub fn owned_workspaces(&self) -> Vec<Uuid> {
        self.facts()
            .get::<OwnedWorkspaces>()
            .map(|OwnedWorkspaces(owned)| owned.clone())
            .unwrap_or_default()
    }
}

impl Principal for AppPrincipal {
    fn id(&self) -> PrincipalId {
        self.id
    }

    fn passport(&self) -> &Passport {
        &self.passport
    }
}

impl PassportPrincipal for AppPrincipal {
    fn from_passport(
        _pg: &PgPool,
        passport: Passport,
    ) -> BoxFuture<'_, Result<Self, PrincipalRejected>> {
        Box::pin(async move {
            let id = passport
                .user_id()
                .or_else(|| passport.service_account_id())
                .ok_or_else(|| PrincipalRejected::new("the passport names no subject"))?;
            Ok(AppPrincipal {
                id: PrincipalId::from(id),
                passport,
                facts: PrincipalFacts::new(),
            })
        })
    }
}

pub struct AppPrincipalResolver;

impl PrincipalResolver<AppPrincipal> for AppPrincipalResolver {
    fn resolve<'a>(
        &'a self,
        _pg: &'a PgPool,
        current: &'a AppPrincipal,
    ) -> BoxFuture<'a, Result<Option<AppPrincipal>, EngineError>> {
        Box::pin(async move {
            Ok(Some(AppPrincipal {
                id: current.id,
                passport: current.passport.clone(),
                facts: PrincipalFacts::new(),
            }))
        })
    }
}
