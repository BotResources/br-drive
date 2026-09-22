use std::sync::Arc;

use futures_util::future::BoxFuture;
use service_engine::Engine;
use service_engine::error::EngineError;

use crate::kernel::{AppPrincipal, HostSettings};

pub fn all(engine: &mut Engine<AppPrincipal>) -> Result<(), EngineError> {
    all_with(Arc::new(HostSettings::default()))(engine)
}

pub fn all_with(
    settings: Arc<HostSettings>,
) -> impl FnOnce(&mut Engine<AppPrincipal>) -> Result<(), EngineError> {
    move |engine| {
        engine.register_principal_resolver(crate::kernel::principal::AppPrincipalResolver)?;
        engine.register_reaction_principal(
            |_pg, actor| -> BoxFuture<'_, Result<AppPrincipal, EngineError>> {
                Box::pin(async move { Ok(AppPrincipal::from_actor(actor)) })
            },
        )?;
        engine.register_principal_fact(move |_pg, principal| {
            let settings = *settings;
            Box::pin(async move {
                principal.facts_mut().insert(settings);
                Ok(())
            })
        })?;
        crate::slices::register(engine)?;
        Ok(())
    }
}
