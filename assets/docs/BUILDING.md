<!--
Copyright 2025 Tabs Data Inc.
-->

![TabsData](/assets/images/tabsdata.png)

# Building Tabsdata

## Supported Operative Systems

* Windows (x86 - latest)
* OSX (Apple silicon/x86 - latest)
* Ubuntu, Debian & RedHat - (x86 - latest)

## Requirements

* Python (3.12)
* Rust (1.84.0)
    * Cargo
    * Cargo packages:
    * cargo-audit
    * cargo-deny
    * cargo-license
    * cargo-machete
    * cargo-make
    * cargo-nextest
    * cargo-pants
    * cargo-update
    * cross

## Requirements for Running Tests

* Docker (latest)
* Graphbiz (latest)
* Powershell (latest)
* Oracle Instant Client  (all components) (latest)

## Required Environment Variables

* `PYTEST_MAXIMUM_RETRY_COUNT=10`
* `MARKERS=""`
* To run Oracle connectors tests the following environment variables must be set/exported.
  If Oracle client is installed in `<ORACLE_CLIENT_PATH>`.

    ```
    PATH="${PATH}:<ORACLE_CLIENT_PATH>"
    LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:<ORACLE_CLIENT_PATH>"
    ORACLE_HOME=<ORACLE_CLIENT_PATH>
    DYLD_LIBRARY_PATH=<ORACLE_CLIENT_PATH>
    TNS_ADMIN=<ORACLE_CLIENT_PATH>
    ```

## Get a Local Copy of Tabsdata Repository

```
git clone https://github.com/tabsdata/tabsdata
```

## Install Python Dependencies for Development

From Tabsdata local repo root directory, run:

```
pip install -r requirements-dev.txt
```

## Basic Build Commands

### Clean Tabsdata Build (target) Directory

```
cargo make clean
```

### Build Tabsdata

```
cargo make build
```

The build will be available under `<tabsdata>/target/`

### Run Tabsdata Tests

#### Run all tests:

```
cargo make test
```

#### Run Python tests:

```
cargo make test_py
```

#### Run Rust tests:

```
cargo make test_rs
```

### Create Tabsdata Python Package (Wheel)

```
cargo make assembly
```

The Python package will be available under `<tabsdata>/target/python/dist/`

### Available Build Profiles

* `debug` (default): debug build.
* `release`: standard optimization build.
* `assembly`: maximum optimization build.
* `integration`: used in integration as a fast replacement of release builds.

Use `--env profile=<PROFILE>` with the `cargo` commands, for example:

```
cargo make clean
cargo make --env profile=assembly build
cargo make --env profile=assembly assembly
```

### List All Available Build Commands

```
cargo make --list-all-steps
```

## Running a Local Build

After doing a build (no need to create the Python package -assembly-).

### Install the Project Python packages

From Tabsdata repository root directory run:

```
pip install ".[test]"
```

### Start the Server and Check It Is Running

```
tdserver start
tdserver status
```

### Using the `td` Client Command Line Tool

For registering and updating functions in Tabsdata server use the following option
for Tabsdata server to use the correct Python packages, `--local-pkg <TABSDATA_REPO_ROOT_PATH>`.

For more detail on how to use Tabsdata server please refer to
[Tabsdata User Guide](https://docs.tabsdata.com/latest/api_ref/index.html).
