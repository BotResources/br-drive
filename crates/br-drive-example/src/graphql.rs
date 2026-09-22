use std::sync::Arc;

use axum::Router;
use service_engine::ReadinessHandle;
use service_engine::{Engine, engine_schema};

use crate::kernel::AppPrincipal;
use crate::slices::{MutationRoot, QueryRoot, SubscriptionRoot};

pub fn build(engine: &mut Engine<AppPrincipal>, readiness: ReadinessHandle) -> Router {
    let state = Arc::new(engine.graphql_state());
    let schema = engine_schema(
        QueryRoot::default(),
        MutationRoot::default(),
        SubscriptionRoot::default(),
        state.clone(),
    );
    engine.set_schema_sdl(schema.sdl());
    service_engine::app(schema, state, readiness)
}
