//! The host's caller admission, asked by every root of `drive_slice!` before
//! anything else. Not a host API: the macro expansion calls these.

use async_graphql::{Context, Error};
use service_engine::MutationError;
use service_engine::gate::Reason;

use crate::host::DriveHost;

const NOT_ADMITTED: &str = "the host does not admit this caller";

/// The admission of the principal on `ctx`, or `None` when there is none:
/// a request without a principal falls through to the root, which answers it
/// as it always did.
fn refusal<H: DriveHost>(
    ctx: &Context<'_>,
    ask: impl FnOnce(&H) -> Option<Reason>,
) -> Option<Reason> {
    ctx.data_opt::<H>().and_then(ask)
}

/// A query root's admission: a refusal is the gate's reason as `code`.
#[doc(hidden)]
pub fn admitted_query<H: DriveHost>(ctx: &Context<'_>) -> Result<(), Error> {
    match refusal::<H>(ctx, |principal| principal.admit().reason()) {
        None => Ok(()),
        Some(reason) => Err(service_engine::coded_error(reason.code(), NOT_ADMITTED)),
    }
}

/// A mutation root's admission, refused as the pipeline refuses a gesture:
/// the gate's reason as `code`, before any load, record or publication.
#[doc(hidden)]
pub fn admitted_mutation<H: DriveHost>(ctx: &Context<'_>) -> Result<(), Error> {
    match refusal::<H>(ctx, |principal| principal.admit().reason()) {
        None => Ok(()),
        Some(reason) => Err(MutationError::refused(reason, NOT_ADMITTED).into()),
    }
}

/// A subscription root's admission, asked before the stream is attached.
#[doc(hidden)]
pub fn admitted_subscription<H: DriveHost>(ctx: &Context<'_>) -> Result<(), Error> {
    #[allow(deprecated)]
    let ask = |principal: &H| principal.admit_subscription().reason();
    match refusal::<H>(ctx, ask) {
        None => Ok(()),
        Some(reason) => Err(service_engine::coded_error(reason.code(), NOT_ADMITTED)),
    }
}
