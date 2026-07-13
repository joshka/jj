use jj_lib::repo::StoreFactories;

use crate::CumulusBackend;
use crate::CumulusOpHeadsStore;
use crate::CumulusOpStore;

/// Returns the Cumulus backend, operation-store, and op-heads factories.
pub fn store_factories() -> StoreFactories {
    let mut factories = StoreFactories::empty();
    factories.add_backend(
        CumulusBackend::name(),
        Box::new(|_settings, path| Ok(Box::new(CumulusBackend::load(path)?))),
    );
    factories.add_op_store(
        CumulusOpStore::name(),
        Box::new(|_settings, path, root_data| Ok(Box::new(CumulusOpStore::load(path, root_data)?))),
    );
    factories.add_op_heads_store(
        CumulusOpHeadsStore::name(),
        Box::new(|_settings, path| Ok(Box::new(CumulusOpHeadsStore::load(path)?))),
    );
    factories
}
