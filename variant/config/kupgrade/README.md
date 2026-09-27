<!--
Copyright 2026 Tabsdata Inc.
-->

# Upgrade templates

What an instance of each released version holds in its configmaps, one folder
per provider: `<version>/<provider>/{configs,pods,assets}/<file>`, one resolved
file per bundle. Config schemas and foreign assets are left out: they come from
the running binary, never from the configmaps.

The replay (`tdk_kupgrader replay`, and the td-kubernetes `upgrade::replay`
tests) takes each provider's oldest template and upgrades it hop by hop with the
shipped upgraders, comparing every hop with the next version's template. The
tests also fail when the inventory head is not the version being built, or when
a supported provider lacks templates from its first version up to the head.

## Generation

`cargo make upgrader templates` resolves each version's kprofile with the
bundle rules of `tdk_kupgrader`, for every supported provider it holds variants
for, and compares the result with these folders; `--write` rewrites them, and
`--version <version>` narrows it to one version.

- The head version resolves the working kprofile. `cargo make upgrader build`
  rewrites it, and `cargo make upgrader check` fails while it is stale.
- Every other version resolves the kprofile of its git tag.
  `patches/<version>.patch` holds what the published package of a version
  changed on top of its tag, and is applied before resolving, so the templates
  hold what instances of that version really run.

The published packages were checked against this: resolving their kprofile
matches `tdkserver init` of the same package, and `config get` of a running
instance holds the same configs.
