<!--
Copyright 2025 Tabs Data Inc.
-->

![TabsData](/assets/images/tabsdata.png)

# Development Process

## Create a GitHub Issue

The `Title` should mention the bug, problem or feature

The `Description` should add all necessary details.

Follow up comments as needed.


## Create Pull Request addressing the GitHub Issue

Assign the issue to yourself, create a pull request.


The first line of the commit message should be

```
[TAB-#GH_ISSUE#] #GH_ISSUE_TITLE#
```

The rest of the commit message should mention any detail about the
issue that could be useful to understand the issue to somebody
looking at the commit messages. It should also described how the
issue has been addressed and what kind of testing has been done.

Finally, the commit message should end with:

```
  fixes #GH_ISSUE#
```

This will trigger the closing of the issue as soon as the pull request is merge.

**References:**

*  [Linking GH issues to a PR](https://docs.github.com/en/issues/tracking-your-work-with-issues/using-issues/linking-a-pull-request-to-an-issue)

## GitHub Action CI Workflow on Pull Request

Wait Until the CI GitHub Action Workflow validates the pull request.

Iterate updating the pull request as needed.

## Code Review on Pull Request

Wait for reviewers (at least two) to review the pull request.

Iterate updating the pull request as needed.

## Merge Pull Request

Once the pull request has been approved by CI and 2 developers,
the pull request can be merged.

## Complex Issues

When dealing with with complex issues, create sub issues as needed breaking
the resolution into small stacked up pull request addressing parts of the
overall issue.

Keep rebasing them on top of the `main` branch to keep them current.

Coordinate with other developers that are working together to
interlace pull request as needed.

