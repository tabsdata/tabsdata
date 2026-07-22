//
// Copyright 2026 Tabsdata Inc.
//

//! Operator-driven logic-layer endpoints.
//!
//! Replay re-executes a finished LFR by minting a fresh concrete FR +
//! output TDV placeholders against the latest patch of the LFR's
//! planned function version. Pointer redirection happens at worker
//! finish (on success only) — at replay time the LFR pointer keeps
//! pointing at FR_old so downstream consumers continue to read the
//! prior-committed data while the new attempt runs.

pub mod layers;
pub mod services;
