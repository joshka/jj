//! Cumulus client stores and Mode A synchronization.
//!
//! [`CumulusBackend`] keeps immutable objects in a local SQLite+CAS cache and
//! fetches cache misses from `cumulusd`. Network futures always run on the
//! backend-owned Tokio runtime, so callers may poll jj-lib traits from any
//! executor.

#![warn(missing_docs)]

mod backend;
mod config;
mod factories;
mod op_heads;
mod op_store;
mod remote;
mod sync;

pub use backend::CumulusBackend;
pub use config::CumulusConfig;
pub use config::CumulusConfigError;
pub use factories::store_factories;
pub use op_heads::CumulusOpHeadsStore;
pub use op_store::CumulusOpStore;
pub use remote::RemoteConfigError;
pub use remote::fetch_remote_config;
pub use sync::SyncEngine;
pub use sync::SyncError;
pub use sync::SyncReport;
pub use sync::SyncStatus;
