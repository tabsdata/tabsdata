//
// Copyright 2026 Tabs Data Inc.
//

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ClusterStartOutcome {
    Started,
    AlreadyRunning,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ClusterStopOutcome {
    Stopped,
    AlreadyStopped,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum InstanceStartOutcome {
    Started,
    AlreadyRunning,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum InstanceStopOutcome {
    Stopped,
    AlreadyStopped,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ClusterRestartOutcome {
    pub stop: Option<ClusterStopOutcome>,
    pub start: ClusterStartOutcome,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct InstanceRestartOutcome {
    pub stop: Option<InstanceStopOutcome>,
    pub start: InstanceStartOutcome,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ServerStartOutcome {
    pub cluster: ClusterStartOutcome,
    pub instance: InstanceStartOutcome,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ServerStopOutcome {
    pub cluster: ClusterStopOutcome,
    pub instance: InstanceStopOutcome,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ServerRestartOutcome {
    pub stop: Option<ServerStopOutcome>,
    pub start: ServerStartOutcome,
}
