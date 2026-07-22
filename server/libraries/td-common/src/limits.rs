//
// Copyright 2026 Tabsdata Inc.
//

//! POSIX resource limits — read the current process's `getrlimit` values
//! for any [`Resource`] (NOFILE, NPROC, AS, CPU, ...).
//!
//! The wrapping is intentionally thin: it returns the soft + hard pair
//! the same way `getrlimit` does, just typed as [`Limits`] instead of a
//! `(u64, u64)` tuple so call sites read naturally.

use std::io;

pub use rlimit::Resource;

/// Soft and hard caps for a single resource. `u64` matches the
/// `rlim_t` shape on every supported platform, and is wide enough for
/// `INFINITY` sentinels (`rlimit::INFINITY`) without lossy casts.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Limits {
    pub soft: u64,
    pub hard: u64,
}

/// Return the current process's soft + hard limits for `resource`.
///
/// Mirrors `getrlimit(resource, &mut rl)` and packages the result as
/// [`Limits`]. Errors propagate the kernel's `errno` via `io::Error`.
pub fn limit(resource: Resource) -> io::Result<Limits> {
    let (soft, hard) = resource.get()?;
    Ok(Limits { soft, hard })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nofile_limits_are_readable() {
        let limits = limit(Resource::NOFILE).expect("getrlimit NOFILE");
        assert!(
            limits.soft <= limits.hard,
            "soft={} must be <= hard={}",
            limits.soft,
            limits.hard,
        );
    }

    #[test]
    fn nproc_limits_are_readable() {
        // NPROC is defined on macOS and Linux.
        let limits = limit(Resource::NPROC).expect("getrlimit NPROC");
        assert!(limits.soft <= limits.hard);
    }
}
