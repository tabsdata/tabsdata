<!--
Copyright 2026 Tabsdata Inc.
-->

# T2 - Function Runner

Function runners perform multiple tasks when running a publisher, subscriber,
transformer or stage trigger function. Many of these tasks are shared and some
are specific to the function type.

For example, a publisher run performs the following tasks:

1. Fetch the definition (the request.yaml)
2. Prepare the runtime environment
3. Localize the function bundle
4. Localize the function state (initial values)
5. Run the source plugin getting all data in parquet files
6. Run the function
7. Upload the function state to storage and run the destination plugin
8. ReportCompletion (the response.yaml)

In Tabsdata 1.0 all of this is done in-process by the function runner, which
forks helper processes (e.g. the transporter) for some of these tasks.

The function runner acts as both orchestrator and executor. On a crash, it is
hard to report exactly what happened, and as we add functionality the
monolithic design makes changes harder and riskier.

As we move to k8s, we can offload the orchestration responsibilities to k8s
using a composite task pattern.

## T2 Publisher Fairy Tale Story

The API Server determines that a function run in an execution graph is ready
for execution and calls its function dispatcher with the function run ID
(exec plan ID, transaction ID, function ID).

Tabsdata storage credentials, stored as k8s secrets, are attached as secrets
for tasks #3, #4 and the state-upload portion of task #7.

The publisher source plugin secrets, also stored as k8s secrets, are then
attached as secrets for task #5.

The publisher destination plugin secrets, also stored as k8s secrets, are
attached as secrets for the destination-plugin portion of task #7.

A function run access key is generated.

A k8s Job Pod definition is created with containers that perform the tasks
above.

The definition is a publisher template with the following specific inputs:

* A URL to fetch the publisher definition (request.yaml)
* A unique access key secret (to access the given URL)
* The necessary secrets to access Tabsdata storage
* The necessary secrets to run the source plugin
* The necessary secrets to run the destination plugin

The rest of the template runs the tasks above, exposing each secret only to
the task that needs it.

Task #1 fetches the definition, and the remaining tasks act on it to fulfill
their responsibilities. At this point things are not much different from what
the Tabsdata 1.0 function runner does.

## Implementation Details

### T2 API Server New Responsibilities

* Determine the secrets required by a function run for the k8s Job Pod definition
* Create a Function Run Access Key
* Provide a protected REST endpoint that returns the definition (request.yaml)

### Function Run Required Secrets

Using the function ID, the API Server fetches the publisher definition (the
JSON representation of the publisher decorator), which the API Server now
stores during function registration.

The JSON representation is scanned for `NamedConn` entries, which are replaced
with the actual connections.

In a follow-up pass, an ENV VAR resolver resolves `EnvRefs` on the API Server.

In a third pass, `SecretRefs` are collected and replaced with `EnvRefs` backed
by synthesized ENV VARs (the API Server does not resolve them). The secrets
are also validated against k8s.

The *resolved* publisher JSON definition is stored as a k8s config map for
the Job Pod (no actual secrets are stored there).

When running the function source or destination plugin (task #5 and the
destination-plugin portion of task #7), the task has access to the JSON
definition config map; loading it resolves all secrets, because they are
already seeded as ENV VARs.

### Transporter Run Required Secrets

The transporter `request.yaml` already expects credentials as ENV VARs, which
have been seeded in the k8s Job Pod definition.

### Prep Intra-Tasks

In the publisher tasks above, task #4 and the state-upload portion of task
#7 involve creating a transporter `request.yaml` file and invoking the
transporter with it.

We could split each into two separate tasks: one that creates the
`request.yaml`, and a second that is a plain transporter invocation.

Task #7's destination-plugin portion is a separate sub-task that runs the
destination plugin against the function's output tables.

## What Tasks the Other Functions Would Have

### Stream Run

1. Fetch the definition (the request.yaml)
2. Prepare the runtime environment
3. Localize the function bundle
4. Run Trigger Plugin
5. ReportCompletion (on error)

### Transformer Run

1. Fetch the definition (the request.yaml)
2. Prepare the runtime environment
3. Localize the function bundle
4. Localize the function state (initial values, input tables)
5. Run the function
6. Upload the function output tables and state to storage
7. ReportCompletion (the response.yaml)

### Subscriber Run

1. Fetch the definition (the request.yaml)
2. Prepare the runtime environment
3. Localize the function bundle
4. Localize the function state (initial values, input tables)
5. Run the function
6. Run the source plugin getting all data in parquet files
7. Upload the function state to storage
8. ReportCompletion (the response.yaml)

### Cron Run

1. Invoke function trigger
