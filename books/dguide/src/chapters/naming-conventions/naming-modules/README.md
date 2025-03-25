<!--
Copyright 2025 Tabs Data Inc.
-->

# Naming Modules

We follow the following conventions to name crates, packages, modules and other kind of components that help to organize the whole code into
self-contained concerns:

- **ta-**: Used as a prefix to name an API component describing the interface of other components.
- **tc-**: Used as a prefix to name connectors (a built-in module enriching the core pub/sub components). 
- **td-**: Used as a prefix to name a generic library component. 
- **te-**: Used as a prefix to name an extension (a component having many implementations depending on the product's edition). 
- **tm-**: Used as a prefix to name a macro crate.
- **to-**: Used as a prefix to name pyo3 crates. 
- **tp-**: Used as a prefix to name plugins (a third-party module enriching the core pub/sub components).
- **ty-**: Used as a prefix to name polars extensions.