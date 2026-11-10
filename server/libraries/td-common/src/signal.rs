//
//  Copyright 2024 Tabsdata Inc.
//

use sysinfo::Signal;
use tokio::select;
use tracing::info;

/// The termination signals a program stops on, received from the moment they
/// are installed: one arriving before [`Signals::wait`] runs is kept for it,
/// instead of meeting the default action, which ends the process at once. A
/// program installs them before it starts anything it has to stop, and only
/// if it then waits for them: one installed and never waited for is lost.
pub struct Signals {
    #[cfg(not(windows))]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(not(windows))]
    terminate: tokio::signal::unix::Signal,
    #[cfg(not(windows))]
    quit: tokio::signal::unix::Signal,
    #[cfg(not(windows))]
    hangup: tokio::signal::unix::Signal,
    #[cfg(windows)]
    ctrl_c: tokio::signal::windows::CtrlC,
    #[cfg(windows)]
    ctrl_break: tokio::signal::windows::CtrlBreak,
    #[cfg(windows)]
    ctrl_close: tokio::signal::windows::CtrlClose,
    #[cfg(windows)]
    ctrl_logoff: tokio::signal::windows::CtrlLogoff,
    #[cfg(windows)]
    ctrl_shutdown: tokio::signal::windows::CtrlShutdown,
}

impl Signals {
    // https://www.gnu.org/software/libc/manual/html_node/Termination-Signals.html
    #[cfg(not(windows))]
    pub fn install() -> Self {
        use tokio::signal::unix::{SignalKind, signal};

        Self {
            interrupt: signal(SignalKind::interrupt())
                .expect("Failed to install the SIGINT handler"),
            terminate: signal(SignalKind::terminate())
                .expect("Failed to install the SIGTERM handler"),
            quit: signal(SignalKind::quit()).expect("Failed to install the SIGQUIT handler"),
            hangup: signal(SignalKind::hangup()).expect("Failed to install the SIGHUP handler"),
        }
    }

    // https://learn.microsoft.com/en-us/windows/console/handlerroutine
    #[cfg(windows)]
    pub fn install() -> Self {
        use tokio::signal::windows;

        Self {
            ctrl_c: windows::ctrl_c().expect("Failed to install the Ctrl+C handler"),
            ctrl_break: windows::ctrl_break().expect("Failed to install the Ctrl+Break handler"),
            ctrl_close: windows::ctrl_close().expect("Failed to install the Ctrl+Close handler"),
            ctrl_logoff: windows::ctrl_logoff().expect("Failed to install the Ctrl+Logoff handler"),
            ctrl_shutdown: windows::ctrl_shutdown()
                .expect("Failed to install the Ctrl+Shutdown handler"),
        }
    }

    /// The signal received, or None if no more can be. SIGHUP is ignored: the
    /// program outlives the terminal that started it.
    #[cfg(not(windows))]
    pub async fn wait(mut self) -> Option<Signal> {
        loop {
            select! {
                result = self.interrupt.recv() => return received("SIGINT", result, Signal::Interrupt),
                result = self.terminate.recv() => return received("SIGTERM", result, Signal::Term),
                result = self.quit.recv() => return received("SIGQUIT", result, Signal::Kill),
                result = self.hangup.recv() => {
                    if result.is_none() {
                        info!(signal = "SIGHUP", "No more SIGHUP signals can be received");
                        return None;
                    }
                    info!(
                        signal = "SIGHUP",
                        "Ignoring SIGHUP, as the process outlives the terminal that started it"
                    );
                },
            }
        }
    }

    /// The signal received, or None if no more can be. On Windows, stopping
    /// workers is always forceful (see `td_common::os::kill_with`), whatever
    /// the signal.
    #[cfg(windows)]
    pub async fn wait(mut self) -> Option<Signal> {
        select! {
            result = self.ctrl_c.recv() => received("Ctrl+C", result, Signal::Term),
            result = self.ctrl_break.recv() => received("Ctrl+Break", result, Signal::Term),
            result = self.ctrl_close.recv() => received("Ctrl+Close", result, Signal::Term),
            result = self.ctrl_logoff.recv() => received("Ctrl+Logoff", result, Signal::Term),
            result = self.ctrl_shutdown.recv() => received("Ctrl+Shutdown", result, Signal::Term),
        }
    }
}

fn received(name: &'static str, result: Option<()>, signal: Signal) -> Option<Signal> {
    if result.is_none() {
        info!(signal = name, "No more {name} signals can be received");
        return None;
    }
    if signal == Signal::Kill {
        info!(signal = name, "Received {name}; initiating a forceful stop");
    } else {
        info!(signal = name, "Received {name}; initiating a graceful stop");
    }
    Some(signal)
}

#[cfg(all(test, not(windows)))]
mod tests {
    use super::*;
    use std::time::Duration;

    // One test, as a signal raised reaches every test's handlers
    #[tokio::test]
    async fn a_signal_before_the_wait_is_kept_and_sighup_is_ignored() {
        let signals = Signals::install();
        unsafe {
            libc::raise(libc::SIGTERM);
        }
        let received = tokio::time::timeout(Duration::from_secs(10), signals.wait())
            .await
            .expect("The signal sent before the wait was lost");
        assert_eq!(received, Some(Signal::Term));

        let signals = Signals::install();
        unsafe {
            libc::raise(libc::SIGHUP);
        }
        let mut wait = Box::pin(signals.wait());
        assert!(
            tokio::time::timeout(Duration::from_millis(500), wait.as_mut())
                .await
                .is_err(),
            "The wait ended on SIGHUP"
        );
        unsafe {
            libc::raise(libc::SIGTERM);
        }
        let received = tokio::time::timeout(Duration::from_secs(10), wait)
            .await
            .expect("The wait missed SIGTERM");
        assert_eq!(received, Some(Signal::Term));
    }
}
