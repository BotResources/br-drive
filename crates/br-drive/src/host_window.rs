//! The live window over a host-wide catalogue (labels, rulesets): gated per
//! principal by the host, so it follows the principal's facts as well as the
//! catalogue — a principal who gains or loses the host's read gate sees its
//! window repopulate at once, not at the next catalogue change.

use std::collections::BTreeSet;
use std::sync::Arc;

use service_engine::impact::{Deps, Dims, Impact};
use service_engine::population::{Interest, Population, WindowQuery};
use service_engine::wire::Noun;
use uuid::Uuid;

pub(crate) fn catalogue_window<N: Noun<Key = Uuid>>(keys: BTreeSet<Uuid>) -> Population<Uuid> {
    let interest = Interest::new()
        .on_noun(N::NAME, Dims::EMPTY)
        .on_deps(Deps::ALL);
    Population::Query(
        WindowQuery::new(interest, Arc::new(|_: &Uuid, _: &Impact| false)).with_keys(keys),
    )
}
