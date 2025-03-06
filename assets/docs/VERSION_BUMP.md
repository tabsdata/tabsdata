<!--
Copyright 2025 Tabs Data Inc.
-->

![TabsData](/assets/images/tabsdata.png)

# Version Bump

The general procedure to bump a new version is:


- Tag all the repositories with the current version just after a new release.
- From all the code repositories, run the command
```
cargo make bump <NEW_VERSION>
```
To run this command you will need an active python environment, and the cargo 
tool ```cargo-workspaces``` that you can install with command:
```
cargo install cargo-workspaces
```
This will update version in all Cargo.toml files (when necessary) and in all 
registered files requiring a version update. Each project contains a file 
```
.custom/bump.cfg
```
containing a lis of such files.

This tool will also warn about any file containing the former version but not
tagged as requiring update. This is not necessarily an error ,but attention should 
be paid to any warning.
- Once the task finishes, you need to submit the changes on any repository with 
changed files.

