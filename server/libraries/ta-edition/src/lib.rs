//
// Copyright 2026 Tabsdata Inc.
//

//! What an edition contributes to the binaries, as typed values: the schema
//! components and baseline seeders of td_dbmanager and the routes of
//! td_apiserver. `te-edition` builds one by calling into each edition crate,
//! and every binary entry takes it, so an edition that is not wired in does
//! not compile.

use ta_apiserver::router::{RouterRegistration, Routes};
use td_database::components::{BaselineSeeder, Registry, SchemaComponent};

#[derive(Clone, Copy)]
pub struct Edition {
    pub components: &'static [&'static dyn SchemaComponent],
    pub seeders: &'static [BaselineSeeder],
    pub routes: &'static [RouterRegistration],
}

impl Edition {
    /// Nothing beyond what the open-source crates carry themselves.
    pub const EMPTY: Edition = Edition {
        components: &[],
        seeders: &[],
        routes: &[],
    };

    /// The schema components and seeders td_dbmanager manages, the built-in
    /// apiserver component included.
    pub fn database(&self) -> Registry {
        Registry::new(self.components, self.seeders)
    }

    /// The routes td_apiserver serves, its own included.
    pub fn routes(&self) -> Routes {
        Routes::new(self.routes)
    }
}
