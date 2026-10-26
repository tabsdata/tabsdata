<!--
Copyright 2025 Tabsdata Inc.
-->

<div align="center">
    <picture>
        <source media="(prefers-color-scheme: dark)" srcset="../images/tabsdata-logo-for-dark-theme.svg">
        <source media="(prefers-color-scheme: light)" srcset="../images/tabsdata-logo-for-light-theme.svg">
        <img src="../images/tabsdata-logo-adaptive.svg" alt="Tabsdata" width="500">
    </picture>
</div>

# GitHub Actions Workflows

## Running Locally

- Install **act**:

```
brew install act
```
- Run the integration workflow triggered by a dispatch event:

```
act --container-architecture linux/arm64 --secret GITHUB_TOKEN="$(gh auth token)" -W ./.github/workflows/integration.yml --eventpath .github/events/integration.json workflow_dispatch
```