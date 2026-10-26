<!--
Copyright 2025 Tabsdata Inc.
-->

<div align="center">
    <img src="../images/tabsdata-logo-for-light-theme.svg#gh-light-mode-only" alt="Tabsdata" width="500">
    <img src="../images/tabsdata-logo-for-dark-theme.svg#gh-dark-mode-only" alt="Tabsdata" width="500">
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