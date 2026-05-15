# T2 - Publisher as Init Containers

The publisher tasks from `00-FunctionRunner.md` run as a chain of init
containers in a single k8s Job Pod. Containers share one volume to exchange
files, and each container receives only the environment variables it needs.

All commands follow `01-CommandBasics.md`.

## Venv activation: pod-runner responsibility

`prepare-py-env` (§2) is the only command that produces or reads
`run-env.yaml`. The **pod-runner** (the orchestrator running the
init-container chain — k8s, the local k- harness, etc.) consumes
`run-env.yaml` after §2 and, for every subsequent container, invokes the
command in a process that already has the virtual environment active
(typically by setting `command:` to `/mount/td/venv/bin/python -m
<command>` or by activating the venv before exec).

The venv always has `tabsdatak` installed, plus the function's resolved
dependencies. Therefore steps 3 onwards do **not** take `--run-env`, do
**not** parse run-env.yaml, and do **not** spawn a subprocess of the venv
themselves — they just run in whatever Python the pod-runner gave them.

## Shared Pod Volume

An `emptyDir` is mounted at `$(ROOT)/mount/td` in every container. All path
options on the commands below are written as `$(ROOT)/mount/td/...` so they
resolve to the mount root regardless of how `ROOT` is set.

Containers leave artifacts there for downstream containers to pick up:

* `$(ROOT)/mount/td/request.yaml` — function run request (the resolved definition).
* `$(ROOT)/mount/td/run-env.yaml` — prepared Python runtime descriptor.
* `$(ROOT)/mount/td/bundle/` — expanded function bundle.
* `$(ROOT)/mount/td/input/` — input tables (parquet).
* `$(ROOT)/mount/td/state/in/`, `$(ROOT)/mount/td/state/out/` — function state in and out.
* `$(ROOT)/mount/td/output/` — output tables produced by the function.
* `$(ROOT)/mount/td/transporter/<task>.yaml` — transporter request files.
* `$(ROOT)/mount/td/response.yaml` — function run response.
* `$(ROOT)/mount/td/liveness` — liveness file, touched by the running container.

## Container Chain

### 1. fetch-request

```
fetch-request
  --url env:FN_RUN_REQUEST_URL
  --token env:FN_TOKEN
  --out $(ROOT)/mount/td/request.yaml
  --liveness $(ROOT)/mount/td/liveness
```

Envs: `FN_TOKEN` (function run access key).

### 2. prepare-py-env

```
prepare-py-env
  --request $(ROOT)/mount/td/request.yaml
  --out $(ROOT)/mount/td/run-env.yaml
  --liveness $(ROOT)/mount/td/liveness
```

Envs: none.

### 3. localize-fn-bundle

```
localize-fn-bundle
  --request $(ROOT)/mount/td/request.yaml
  --dir $(ROOT)/mount/td/bundle
  --liveness $(ROOT)/mount/td/liveness
```

Envs: Tabsdata storage credentials.

### 4a. prep-localize-state

Generates the transporter request used in 4b.

```
prep-localize-state
  --request $(ROOT)/mount/td/request.yaml
  --in-dir $(ROOT)/mount/td/input
  --state-dir $(ROOT)/mount/td/state/in
  --out $(ROOT)/mount/td/transporter/localize-state.yaml
  --liveness $(ROOT)/mount/td/liveness
```

Envs: none.

### 4b. localize-state (plain transporter)

```
run-transporter
  --request $(ROOT)/mount/td/transporter/localize-state.yaml
  --liveness $(ROOT)/mount/td/liveness
```

Envs: Tabsdata storage credentials.

### 5. run-source-plugin

```
run-source-plugin
  --request $(ROOT)/mount/td/request.yaml
  --bundle $(ROOT)/mount/td/bundle
  --out-dir $(ROOT)/mount/td/input
  --liveness $(ROOT)/mount/td/liveness
```

Envs: source plugin secrets, injected as the synthesized `TD_S_<#>` env vars
the resolved request refers to.

### 6. run-function

```
run-function
  --request $(ROOT)/mount/td/request.yaml
  --bundle $(ROOT)/mount/td/bundle
  --in-dir $(ROOT)/mount/td/input
  --state-in $(ROOT)/mount/td/state/in
  --out-dir $(ROOT)/mount/td/output
  --state-out $(ROOT)/mount/td/state/out
  --liveness $(ROOT)/mount/td/liveness
```

Envs: none — the user function never sees storage credentials.

### 7a. prep-upload-state

Generates the transporter request used in 7b.

```
prep-upload-state
  --request $(ROOT)/mount/td/request.yaml
  --state-dir $(ROOT)/mount/td/state/out
  --out $(ROOT)/mount/td/transporter/upload-state.yaml
  --liveness $(ROOT)/mount/td/liveness
```

Envs: none.

### 7b. upload-state (plain transporter)

```
run-transporter
  --request $(ROOT)/mount/td/transporter/upload-state.yaml
  --liveness $(ROOT)/mount/td/liveness
```

Envs: Tabsdata storage credentials.

### 7c. run-destination-plugin

```
run-destination-plugin
  --request $(ROOT)/mount/td/request.yaml
  --bundle $(ROOT)/mount/td/bundle
  --out-dir $(ROOT)/mount/td/output
  --liveness $(ROOT)/mount/td/liveness
```

Envs: destination plugin secrets, injected as the synthesized `TD_S_<#>`
env vars the resolved request refers to.

### 8. report-completion

```
report-completion
  --url env:FN_RUN_REPORT_URL
  --token env:FN_TOKEN
  --request $(ROOT)/mount/td/request.yaml
  --out-dir $(ROOT)/mount/td/output
  --state-dir $(ROOT)/mount/td/state/out
  --out $(ROOT)/mount/td/response.yaml
  --liveness $(ROOT)/mount/td/liveness
```

Envs: `FN_TOKEN`.

## Env Scoping Summary

| Container               | Envs                          |
|-------------------------|-------------------------------|
| fetch-request           | `FN_TOKEN`                    |
| prepare-py-env          | —                             |
| localize-fn-bundle      | storage                       |
| prep-localize-state     | —                             |
| localize-state          | storage                       |
| run-source-plugin       | source plugin (`TD_S_*`)      |
| run-function            | —                             |
| prep-upload-state       | —                             |
| upload-state            | storage                       |
| run-destination-plugin  | destination plugin (`TD_S_*`) |
| report-completion       | `FN_TOKEN`                    |

Storage credentials, source plugin secrets, and destination plugin secrets
are scoped to the containers that need them; all other containers run with
no credentials.

## Appendix: Pod YAML Sketch

A representative Job manifest. Two init containers are shown in full; the
rest follow the same pattern.

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: pubrun-<FN_RUN_ID>
spec:
  template:
    spec:
      restartPolicy: Never
      volumes:
        - name: td
          emptyDir: {}
      initContainers:

        - name: fetch-request
          image: tabsdata/runner:<ver>
          workingDir: /mount/td
          command: ["fetch-request"]
          args:
            - --url
            - env:FN_RUN_REQUEST_URL
            - --token
            - env:FN_TOKEN
            - --out
            - $(ROOT)/mount/td/request.yaml
            - --liveness
            - $(ROOT)/mount/td/liveness
          env:
            - name: ROOT
              value: "."   # `.` is for k-; for k8s set to ''
            - name: FN_RUN_REQUEST_URL
              value: https://apiserver/api/v1/request/<FN_RUN_ID>
            - name: FN_TOKEN
              valueFrom:
                secretKeyRef:
                  name: fn-run-<FN_RUN_ID>
                  key: token
          volumeMounts:
            - name: td
              mountPath: /mount/td

        - name: localize-fn-bundle
          image: tabsdata/runner:<ver>
          workingDir: /mount/td
          command: ["localize-fn-bundle"]
          args:
            - --request
            - $(ROOT)/mount/td/request.yaml
            - --dir
            - $(ROOT)/mount/td/bundle
            - --liveness
            - $(ROOT)/mount/td/liveness
          env:
            - name: ROOT
              value: "."   # `.` is for k-; for k8s set to ''
          envFrom:
            - secretRef:
                name: tabsdata-storage-creds
          volumeMounts:
            - name: td
              mountPath: /mount/td

        # prepare-py-env, prep-localize-state, localize-state,
        # run-source-plugin, run-function, prep-upload-state,
        # upload-state, run-destination-plugin, report-completion
        # — same shape: workingDir /mount/td, td volume mounted at
        #   /mount/td, ROOT env defined, env/envFrom scoped per the
        #   table above.

      containers:
        - name: noop
          image: tabsdata/noop:<ver>
          command: ["true"]
```

Notes:

* The `td` `emptyDir` volume is the shared working area; every container
  mounts it at `/mount/td`.
* `ROOT` is defined per container so `$(ROOT)/mount/td/...` expands to the
  right path. In k8s, `ROOT=''` makes `$(ROOT)/mount/td/request.yaml`
  resolve to the absolute `/mount/td/request.yaml`. Outside k8s (k-),
  `ROOT='.'` resolves it to `./mount/td/request.yaml`, relative to a CWD
  that contains the `mount/td` tree.
* Storage credentials are injected with `envFrom: secretRef` so the
  `request.yaml` `EnvRefs` resolve transparently inside the container.
* `FN_TOKEN` is a per-run secret created by the API Server alongside the
  function run access key.
* The Job's main container is a no-op: all real work happens in the init
  container chain, which k8s runs sequentially and fails fast on any error.

## More Notes (for Dimas)

### Pod-runner adjustments

* Drop the injected `PR_ROOT`; we use the `ROOT` env shown above instead.
* Pod-runner must resolve `$(<VAR>)` occurrences in `args` (k8s-style env
  var expansion) so `$(ROOT)/mount/td/...` works.

### Storage credentials layout

Mount credentials are a plain k8s `Secret`:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: tabsdata-storage-creds
type: Opaque
stringData:
  AWS_ACCESS_KEY_ID: AKIAEXAMPLE
  AWS_SECRET_ACCESS_KEY: wJalrXUtnFEMI...
  AWS_REGION: eu-west-1
```

Consumed via `envFrom` (as in the appendix):

```yaml
envFrom:
  - secretRef:
      name: tabsdata-storage-creds
```

The container starts with `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and
`AWS_REGION` already in its environment.

### Pod completion pattern

Instead of latching tricks for container sync, all work goes in
`initContainers` (k8s runs them sequentially) and `containers` is a single
no-op tombstone:

```yaml
spec:
  initContainers:
    - name: step-1
      image: ...
    - name: step-2
      image: ...
    - name: step-3
      image: ...
  containers:
    - name: done
      image: gcr.io/google_containers/pause:3.9
      command: ["/bin/true"]
  restartPolicy: Never
```

The pod transitions Running → Succeeded once the no-op main exits. Downside:
pod status is briefly misleading during the "main running" window, and there
is no true sequential semantics across the main container set. That is fine
here — this is internal pipeline machinery where the dispatcher reads actual
completion state, not pod phase.

### Sidecars (if we add them later)

Sidecars are usable in this layout, with caveats:

* **Position matters.** A sidecar placed third in the init sequence means
  steps 1 and 2 ran without it. Place it before any init container that
  depends on it.
* **Add a `startupProbe`** if subsequent init containers depend on the
  sidecar being *functional*, not just *started*. Without one, kubelet moves
  on as soon as the process starts and can race the sidecar's readiness.
* **Termination is reverse-of-start**, so a sidecar started early lives
  longest. Order sidecars by their shutdown dependencies.
* **Pre-1.29 clusters**: native sidecars are beta in 1.28
  (`SidecarContainers` feature gate) and unavailable before that. Older
  clusters need the legacy "extra container in `containers:` with manual
  lifecycle" hack, which does not compose cleanly with sequential inits.

### Function JSON resolution (replacing pickle)

The publisher definition is now stored as JSON and resolved by the API
Server before the Job is created (see `00-FunctionRunner.md`):

```python
def decorator_info_to_json(info) -> JSON:
    # convert the decorator Info to JSON

def extract_env_refs(json: JSON) -> set[EnvRef]:
    # extract all EnvRefs in the JSON

def resolve_env_refs(refs: list[EnvRef]) -> dict[EnvRef, str]:
    # resolve env var values

def container_envs(resolved: dict[EnvRef, str]) -> dict[str, str]:
    # env names/values the container requires

def extract_secret_refs(json: JSON) -> set[SecretRef]:
    # extract all SecretRefs in the JSON

def map_secret_refs_to_env_refs(
    refs: set[SecretRef],
) -> list[tuple[EnvRef, SecretRef]]:
    # synthesize one env var per secret; ENVs are TD_S_<#>

def replace_secret_refs_with_env_refs(
    json: JSON, mapping: list[tuple[EnvRef, SecretRef]],
) -> JSON:
    # replace all SecretRefs with EnvRefs

def container_env_secrets(
    mapping: list[tuple[EnvRef, SecretRef]],
) -> dict[str, SecretRef]:
    # info to declare secrets as envs in the container defs
```
