<!--
Copyright 2025 Tabs Data Inc.
-->

![TabsData](/assets/images/tabsdata.png)

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