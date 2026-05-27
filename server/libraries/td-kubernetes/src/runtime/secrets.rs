//
// Copyright 2026 Tabs Data Inc.
//

//! Secret generation and synchronization for the kubernetes runtime.
//!
//! Components::
//!
//!     ┌─────────────────────── runtime/secrets ────────────────────────┐
//!     │                                                                │
//!     │   ┌────────────────┐         ┌─────────────────────────────┐   │
//!     │   │  SecretPhase   │         │       KubeContext           │   │
//!     │   │ ─────────────  │         │  ─────────────────────────  │   │
//!     │   │  Create        │         │  client:    Arc<Client>     │   │
//!     │   │  Start         │         │  namespace: String          │   │
//!     │   └────────────────┘         └─────────────────────────────┘   │
//!     │           │                                  │                 │
//!     │           └──────────────┬───────────────────┘                 │
//!     │                          ↓                                     │
//!     │                  ┌────────────────────────┐                    │
//!     │                  │  trait SecretGenerator │                    │
//!     │                  │ ─────────────────────  │                    │
//!     │                  │  name()       -> &str  │                    │
//!     │                  │  phases()     -> &[]   │                    │
//!     │                  │  generate(ctx) -> Map  │                    │
//!     │                  └────────────────────────┘                    │
//!     │                          ▲                                     │
//!     │                          │ implements                          │
//!     │                          │                                     │
//!     │              ┌───────────┴────────────────┐                    │
//!     │              │ KubeconfigSecretGenerator  │                    │
//!     │              │ ────────────────────────── │                    │
//!     │              │ kubefile : PathBuf         │                    │
//!     │              │ phases   : [Start]         │                    │
//!     │              │ name     : "tabsdata-      │                    │
//!     │              │              kubeconfig"   │                    │
//!     │              │                            │                    │
//!     │              │ generate(): reads kubefile │                    │
//!     │              │             encodes bytes  │                    │
//!     │              │             returns        │                    │
//!     │              │             { kubeconfig:  │                    │
//!     │              │                <encoded> } │                    │
//!     │              └────────────────────────────┘                    │
//!     │                          ▲                                     │
//!     │                          │ provided by                         │
//!     │                          │                                     │
//!     │              ┌───────────┴────────────────┐                    │
//!     │              │   KminusProvider           │                    │
//!     │              │   secret_generators(phase) │                    │
//!     │              └────────────────────────────┘                    │
//!     │                          ▲                                     │
//!     │                          │ called by                           │
//!     │                          │                                     │
//!     │              ┌───────────┴────────────────┐                    │
//!     │              │   handle_phase_secrets()   │ (provider.rs)      │
//!     │              │   for each generator:      │                    │
//!     │              │     if phase ∈ phases():   │                    │
//!     │              │       data = generate(ctx) │                    │
//!     │              │       SecretHandler::handle│ ──────────────┐    │
//!     │              └────────────────────────────┘               │    │
//!     │                                                           ↓    │
//!     │                                          ┌──────────────────┐  │
//!     │                                          │  SecretHandler   │  │
//!     │                                          │  handle(ctx,     │  │
//!     │                                          │         phase,   │  │
//!     │                                          │         gens)    │  │
//!     │                                          │   ───────────    │  │
//!     │                                          │   upsert_secret  │  │
//!     │                                          └──────────────────┘  │
//!     │                                                  │             │
//!     └──────────────────────────────────────────────────┼─────────────┘
//!                                                        ↓
//!                                   kube/api/secret.rs ::upsert_secret
//!                                       → cluster.namespace.Secret
//!                                         name: "tabsdata-kubeconfig"
//!                                         data: { kubeconfig: <encoded> }
//!
//! Startup sequence (kminus)::
//!
//!     tdkserver server start --provider kminus
//!             ↓
//!     Provider::start_runtime
//!             ↓
//!       start_cluster()             kminus boots; kubeconfig.yaml on disk
//!             ↓
//!       handle_phase_secrets(Start)
//!             │
//!             │ 1. provider.secret_generators(Start)
//!             │      → [KubeconfigSecretGenerator(kubefile)]
//!             │ 2. provider.kube_config() → kube::Config
//!             │ 3. build kube::Client
//!             │ 4. SecretHandler::handle(ctx, Start, [gen])
//!             │      for gen in [...]:
//!             │        if Start ∈ gen.phases():
//!             │          data = gen.generate(ctx)
//!             │              ← reads kubefile, encodes bytes
//!             │          upsert_secret(client, ns, name, data)
//!             │              ← creates or replaces
//!             │                  Secret "tabsdata-kubeconfig"
//!             ↓
//!       start_instance()            stage configmaps, apply pods
//!                                   apiserver pod boots with env:
//!                                     TDK_KUBECONFIG bound to
//!                                     Secret.data.kubeconfig
//!                                   (codec is internal to td-kubernetes;
//!                                    callers never see encode/decode)

pub mod generator;
pub mod handler;
pub mod kubeconfig;
pub mod phase;

pub use generator::SecretGenerator;
pub use handler::SecretHandler;
pub use kubeconfig::KubeconfigSecretGenerator;
pub use phase::SecretPhase;
