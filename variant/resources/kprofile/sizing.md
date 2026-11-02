<!-- Copyright 2026 Tabsdata Inc. -->

# Container sizing

Requests as `cpu / memory / ephemeral-storage`. Every container declares all
three; `ephemeral-storage` is disk — the writable container layer plus any
`emptyDir` it mounts.

| type | kminus | docker | eks | fargate | why |
|---|---|---|---|---|---|
| apiserver / aiagent | 2 / 4Gi / 4Gi | 2 / 4Gi / 4Gi | 2 / 6Gi / 16Gi | 4 / 8Gi / 16Gi | I/O-bound: HTTP + 50-conn pool. aiagent's only local model is bge-small (~500MB resident); LLMs are remote APIs |
| aiembeddings / aimetadata | 1 / 2Gi / 4Gi | 1 / 2Gi / 4Gi | 2 / 4Gi / 16Gi | 2 / 4Gi / 16Gi | batch embedding with the same small model |
| index-build / index-sync | 1 / 2Gi / 4Gi | 1 / 2Gi / 4Gi | 1 / 2Gi / 16Gi | 2 / 4Gi / 16Gi | sidecars of the aiagent, so they add to its pod total |
| function / source-plugin / destination-plugin steps | 2 / 4Gi / 8Gi | 2 / 4Gi / 8Gi | 4 / 16Gi / 16Gi | 4 / 16Gi / 16Gi | the steps running user code — deserve the headroom |
| other function steps | 500m / 1Gi / 2Gi | 500m / 1Gi / 2Gi | 1 / 2Gi / 8Gi | 1 / 2Gi / 8Gi | fetch/stamp/report orchestration |
| connection-probe (every step) | 250m / 1Gi / 2Gi | 250m / 1Gi / 2Gi | 250m / 1Gi / 2Gi | 250m / 1Gi / 2Gi | one connector call against one connection, `probe` in `tabsdata.yaml`; a file source's metadata probe also reads the heads of up to 200 files (64 KiB each, 8 at a time, within 30 s) for their schemas |
| venv create/download | 1 / 2Gi / 4Gi | 1 / 2Gi / 4Gi | 2 / 4Gi / 16Gi | 2 / 4Gi / 16Gi | 2cpu off kminus: extraction is the bottleneck |
| dbmanager / tdmanager | 1 / 2Gi / 4Gi | 1 / 2Gi / 4Gi | 1 / 4Gi / 16Gi | 1 / 4Gi / 16Gi | schema build or upgrade rules + venv download |
| utility (dbready, apiready, cabundle, connresextractor, inspector) | 50m / 64Mi / 1Gi | 50m / 64Mi / 1Gi | 250m / 512Mi / 2Gi | 250m / 512Mi / 2Gi | Fargate floor is 0.25vCPU/0.5GB |
| database (kminus only) | 1 / 2Gi / 4Gi | — | — | — | embedded postgres |

On Fargate the pod total sizes the microVM and acts as a cap: init containers
count as the maximum of the set, regular containers as the sum. Requests round up
to the next valid size (0.25, 0.5, 1, 2, 4, 8, 16 vCPU, each with a fixed memory
range), so `3 / 5Gi` bills as `4 / 8Gi`.

On EKS a pod is scheduled onto a node rather than into a microVM of its own, so
the pod total has to fit what one node has left. The control path runs on the
always-on node group of `m7i.xlarge`, which allocates 3920m of its 4 vCPU once
kubelet reservations are taken, so **cpu** is what binds there: a request of 4
never schedules, however idle the group is. The apiserver therefore takes 2
rather than the Fargate 4.

Memory is not the constraint. The same node allocates about 14.4Gi, so the 4Gi
the apiserver requests leaves ample room.

Function pods are the exception, keeping the Fargate figures. They run on nodes
Karpenter provisions per pod, admitting nothing below 8 vCPU, so a `4 / 16Gi`
request shapes the node instead of competing for one.

`ephemeral-storage` stays at the Fargate values everywhere on EKS. It is the only
disk-aware scheduling signal there is, and on the function pool it decides which
instance type is launched.

kminus runs every container on the developer's own machine, so its values stay
small enough to schedule alongside an embedded database.
