//
// Copyright 2026 Tabsdata Inc.
//

use td_error::td_error;

#[td_error]
pub enum UpgradeError {
    #[error("Could not parse the upgrade lock: {0}.")]
    LockParseFailed(String) = 5300,
    #[error("Could not render the upgrade lock: {0}.")]
    LockRenderFailed(String) = 5301,
    #[error(
        "Instance '{instance}' is already being upgraded by another actor \
         (lock '{id}' taken at {timestamp}). Wait for it to finish, or clear the lock with \
         'tdkserver upgrade --unlock'."
    )]
    AlreadyLocked {
        instance: String,
        id: String,
        timestamp: String,
    } = 5302,
    #[error(
        "Another actor took the upgrade lock on instance '{instance}' before this one could \
         proceed. Run 'tdkserver upgrade' again."
    )]
    LockLost { instance: String } = 5303,
    #[error("Instance '{instance}' holds no upgrade lock to release.")]
    LockAbsent { instance: String } = 5304,
    #[error("Could not parse version '{version}': {cause}.")]
    VersionParseFailed { version: String, cause: String } = 5310,
    #[error("Could not parse the upgrade inventory: {0}.")]
    InventoryParseFailed(String) = 5311,
    #[error("The upgrade inventory declares schema {found}; this tabsdata supports {supported}.")]
    InventorySchemaUnsupported { found: u32, supported: u32 } = 5312,
    #[error("The upgrade inventory lists no versions.")]
    InventoryEmpty = 5313,
    #[error("The upgrade inventory lists version '{version}' more than once.")]
    InventoryDuplicateVersion { version: String } = 5314,
    #[error(
        "The upgrade inventory is not sorted newest first: '{version}' appears after '{after}'."
    )]
    InventoryNotDescending { version: String, after: String } = 5315,
    #[error(
        "The upgrade inventory declares an upgrader for its oldest version '{version}'; \
         the oldest version has nothing to upgrade from."
    )]
    InventoryFloorHasUpgrader { version: String } = 5316,
    #[error(
        "Instance version '{version}' is older than the oldest upgradable version '{floor}'. \
         Upgrade the instance to '{floor}' with an earlier tabsdata first."
    )]
    VersionBelowFloor { version: String, floor: String } = 5317,
    #[error(
        "Version '{version}' is unknown to this tabsdata, whose newest known version is \
         '{newest}'."
    )]
    VersionNotInInventory { version: String, newest: String } = 5318,
    #[error(
        "The running tabsdata version '{product}' is older than instance version '{instance}'; \
         a newer instance cannot be upgraded by an older tabsdata."
    )]
    ProductOlderThanInstance { product: String, instance: String } = 5319,
    #[error("The upgrade resources path '{0}' is not a folder.")]
    KupgradePathInvalid(String) = 5320,
    #[error("Could not read the upgrade inventory '{path}': {cause}.")]
    InventoryReadFailed { path: String, cause: String } = 5321,
    #[error(
        "The upgrade inventory declares an upgrader for version '{version}', but no spec was \
         found at '{path}'."
    )]
    SpecMissing { version: String, path: String } = 5322,
    #[error(
        "The upgrade inventory declares no upgrader for version '{version}', yet a spec exists \
         at '{path}'."
    )]
    SpecUnexpected { version: String, path: String } = 5323,
    #[error(
        "The upgrade spec '{path}' names version '{version}', which the upgrade inventory does \
         not declare an upgrader for."
    )]
    SpecOrphan { version: String, path: String } = 5324,
    #[error("Could not read the upgrade specs folder '{path}': {cause}.")]
    SpecsReadFailed { path: String, cause: String } = 5325,
    #[error(
        "Instance '{instance}' is part-way through an upgrade to '{target}'; finish it with \
         tabsdata '{target}'. The running tabsdata is '{product}'."
    )]
    TargetMismatch {
        instance: String,
        target: String,
        product: String,
    } = 5326,
    #[error(
        "Instance '{instance}' cannot be started: it is at version '{version}' and an upgrade \
         to '{target}' is under way. Finish it with 'tdkserver upgrade' using tabsdata \
         '{target}'."
    )]
    StartWhileUpgrading {
        instance: String,
        version: String,
        target: String,
    } = 5330,
    #[error(
        "Instance '{instance}' cannot be started: it is only part-way upgraded. Its manifest \
         states version '{version}' while its resources are stamped '{stamped}'. Run \
         'tdkserver upgrade' to finish the upgrade."
    )]
    StartWhilePartiallyUpgraded {
        instance: String,
        version: String,
        stamped: String,
    } = 5331,
    #[error(
        "Instance '{instance}' cannot be started: it is at version '{version}' and the running \
         tabsdata is '{product}'. Run 'tdkserver upgrade', or use tabsdata '{version}'."
    )]
    StartOnAnotherVersion {
        instance: String,
        version: String,
        product: String,
    } = 5332,
    #[error(
        "Instance '{instance}' cannot be started: it states version '{version}' but these \
         resources are at another version: {entries}. Run 'tdkserver upgrade' to finish the \
         upgrade."
    )]
    StartWithStrayEntries {
        instance: String,
        version: String,
        entries: String,
    } = 5333,
    #[error("Could not parse the upgrade spec of version '{version}': {cause}.")]
    SpecParseFailed { version: String, cause: String } = 5340,
    #[error(
        "The upgrade spec of version '{version}' declares schema {found}; this tabsdata \
         supports {supported}."
    )]
    SpecSchemaUnsupported {
        version: String,
        found: u32,
        supported: u32,
    } = 5341,
    #[error("Could not read the upgrade spec of version '{version}' at '{path}': {cause}.")]
    SpecReadFailed {
        version: String,
        path: String,
        cause: String,
    } = 5342,
    #[error(
        "The upgrade spec of version '{version}' writes '{entry}', but no content was found \
         at '{path}'."
    )]
    ContentMissing {
        version: String,
        entry: String,
        path: String,
    } = 5343,
    #[error(
        "The upgrade content '{path}' of version '{version}' is not claimed by any 'create' \
         or 'replace' variant of its spec."
    )]
    ContentOrphan { version: String, path: String } = 5344,
    #[error(
        "The upgrade spec of version '{version}' declares changes for '{entry}', whose action \
         is not 'modify'."
    )]
    SpecChangesUnexpected { version: String, entry: String } = 5346,
    #[error("Could not remove the editable folder '{path}': {cause}.")]
    EditableRemoveFailed { path: String, cause: String } = 5351,
    #[error("The upgrade step names configmap family folder '{folder}', which is not known.")]
    UnknownFamily { folder: String } = 5347,
    #[error(
        "The upgrade to '{version}' modifies '{entry}', but its configmap '{configmap}' does \
         not exist."
    )]
    ModifyTargetMissing {
        version: String,
        entry: String,
        configmap: String,
    } = 5348,
    #[error("Could not parse '{entry}' while upgrading to '{version}': {cause}.")]
    ModifyParseFailed {
        version: String,
        entry: String,
        cause: String,
    } = 5349,
    #[error(
        "The pattern '{pattern}' for '{entry}' in the upgrade to '{version}' is invalid: {cause}."
    )]
    ModifyPatternInvalid {
        version: String,
        entry: String,
        pattern: String,
        cause: String,
    } = 5352,
    #[error(
        "The upgrade to '{version}' asks to merge '{entry}', but the merge action is not \
         implemented yet."
    )]
    MergeNotImplemented { version: String, entry: String } = 5354,
    #[error(
        "The upgrade to '{version}' groups the variant '{variant}' under '{entry}', but it \
         belongs to '{stripped}'."
    )]
    VariantOutsideGroup {
        version: String,
        entry: String,
        variant: String,
        stripped: String,
    } = 5353,
    #[error("Could not render '{entry}' while upgrading to '{version}': {cause}.")]
    ModifyRenderFailed {
        version: String,
        entry: String,
        cause: String,
    } = 5350,
    #[error("Could not read the upgrade content of version '{version}' at '{path}': {cause}.")]
    ContentReadFailed {
        version: String,
        path: String,
        cause: String,
    } = 5345,
}
