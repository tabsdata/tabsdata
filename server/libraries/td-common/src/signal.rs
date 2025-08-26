//
//  Copyright 2024 Tabs Data Inc.
//

use sysinfo::Signal;
use tokio::select;
use tracing::info;

/// Termination signals that can be received by the program. This method is platform-specific,
/// returning the received signal, or None if the monitor ended.
pub async fn terminate() -> Option<Signal> {
    #[cfg(not(windows))]
    // https://www.gnu.org/software/libc/manual/html_node/Termination-Signals.html
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut signal_interrupt_handle = signal(SignalKind::interrupt()).unwrap();
        let mut signal_terminate_handle = signal(SignalKind::terminate()).unwrap();
        let mut signal_quit_handle = signal(SignalKind::quit()).unwrap();

        select! {
            result = signal_interrupt_handle.recv() => {
                if result.is_some() {
                    info!("Received SIGINT (Ctrl+C). Initiating graceful stop...");
                    Some(Signal::Interrupt)
                } else {
                    info!("SIGINT (Ctrl+C) monitor ended.");
                    None
                }
            },
            result = signal_terminate_handle.recv() => {
                if result.is_some() {
                    info!("Received SIGTERM. Initiating graceful stop...");
                    Some(Signal::Term)
                } else {
                    info!("SIGTERM monitor finished.");
                    None
                }
            },
            result = signal_quit_handle.recv() => {
                if result.is_some() {
                    info!("Received SIGQUIT. Initiating forceful stop...");
                    Some(Signal::Kill)
                } else {
                    info!("SIGQUIT monitor finished.");
                    None
                }
            },
        }
    }

    #[cfg(windows)]
    // https://learn.microsoft.com/en-us/windows/console/handlerroutine
    {
        use tokio::signal::windows;

        let mut signal_c_handle = windows::ctrl_c().unwrap();
        let mut signal_break_handle = windows::ctrl_break().unwrap();
        let mut signal_close_handle = windows::ctrl_close().unwrap();
        let mut signal_logoff_handle = windows::ctrl_logoff().unwrap();
        let mut signal_shutdown_handle = windows::ctrl_shutdown().unwrap();

        select! {
            result = signal_c_handle.recv() => {
                if result.is_some() {
                    info!("Received Ctrl+C. Initiating graceful stop...");
                    Some(Signal::Kill)
                } else {
                    info!("Ctrl+C monitor ended.");
                    None
                }
            },
            result = signal_break_handle.recv() => {
                if result.is_some() {
                    info!("Received Ctrl+Break. Initiating graceful stop...");
                    Some(Signal::Kill)
                } else {
                    info!("Ctrl+Break monitor ended.");
                    None
                }
            },
            result = signal_close_handle.recv() => {
                if result.is_some() {
                    info!("Received Ctrl+Close. Initiating graceful stop...");
                    Some(Signal::Kill)
                } else {
                    info!("Ctrl+Close monitor ended.");
                    None
                }
            },
            result = signal_logoff_handle.recv() => {
                if result.is_some() {
                    info!("Received Ctrl+Logoff. Initiating graceful stop...");
                    Some(Signal::Kill)
                } else {
                    info!("Ctrl+Logoff monitor ended.");
                    None
                }
            },
            result = signal_shutdown_handle.recv() => {
                if result.is_some() {
                    info!("Received Ctrl+Shutdown. Initiating graceful stop...");
                    Some(Signal::Kill)
                } else {
                    info!("Ctrl+Shutdown monitor ended.");
                    None
                }
            },
        }
    }
}
