//! Cumulus client stores and Mode A synchronization.
//!
//! [`CumulusBackend`] keeps immutable objects in a local SQLite+CAS cache and
//! fetches cache misses from `cumulusd`. Network futures always run on the
//! backend-owned Tokio runtime, so callers may poll jj-lib traits from any
//! executor.

#![warn(missing_docs)]

mod backend;
mod config;

pub use backend::CumulusBackend;
pub use config::CumulusConfig;
pub use config::CumulusConfigError;
