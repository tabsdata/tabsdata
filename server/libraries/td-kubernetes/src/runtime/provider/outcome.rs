//
// Copyright 2026 Tabsdata Inc.
//

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ClusterStartOutcome {
    Started,
    AlreadyRunning,
    NotSupported,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ClusterStopOutcome {
    Stopped,
    AlreadyStopped,
    NotSupported,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ImagesOutcome {
    Created,
    NotSupported,
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

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ServiceStartOutcome {
    pub cluster: ClusterStartOutcome,
    pub instance: InstanceStartOutcome,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ServiceStopOutcome {
    pub cluster: ClusterStopOutcome,
    pub instance: InstanceStopOutcome,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ServiceRestartOutcome {
    pub stop: Option<ServiceStopOutcome>,
    pub start: ServiceStartOutcome,
}
