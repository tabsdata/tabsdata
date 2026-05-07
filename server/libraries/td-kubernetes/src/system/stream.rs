//
// Copyright 2026 Tabs Data Inc.
//

use std::io::{self, Write};

#[cfg(unix)]
const NULL_DEVICE: &[u8] = b"/dev/null\0";

#[cfg(windows)]
const NULL_DEVICE: &[u8] = b"NUL\0";

#[cfg(unix)]
const STDOUT_FD: i32 = libc::STDOUT_FILENO;
#[cfg(unix)]
const STDERR_FD: i32 = libc::STDERR_FILENO;

#[cfg(windows)]
const STDOUT_FD: i32 = 1;
#[cfg(windows)]
const STDERR_FD: i32 = 2;

pub struct Silencer {
    saved_stdout: Option<i32>,
    saved_stderr: Option<i32>,
}

impl Silencer {
    pub fn new() -> io::Result<Self> {
        let _ = io::stdout().flush();
        let _ = io::stderr().flush();
        let saved_stdout = redirect_to_null(STDOUT_FD)?;
        let saved_stderr = match redirect_to_null(STDERR_FD) {
            Ok(fd) => fd,
            Err(error) => {
                restore(STDOUT_FD, saved_stdout);
                return Err(error);
            }
        };
        Ok(Self {
            saved_stdout: Some(saved_stdout),
            saved_stderr: Some(saved_stderr),
        })
    }
}

impl Drop for Silencer {
    fn drop(&mut self) {
        let _ = io::stdout().flush();
        let _ = io::stderr().flush();
        if let Some(fd) = self.saved_stderr.take() {
            restore(STDERR_FD, fd);
        }
        if let Some(fd) = self.saved_stdout.take() {
            restore(STDOUT_FD, fd);
        }
    }
}

fn redirect_to_null(target_fd: i32) -> io::Result<i32> {
    unsafe {
        let saved = libc::dup(target_fd);
        if saved < 0 {
            return Err(io::Error::last_os_error());
        }
        let null = libc::open(NULL_DEVICE.as_ptr() as *const _, libc::O_WRONLY);
        if null < 0 {
            libc::close(saved);
            return Err(io::Error::last_os_error());
        }
        let result = libc::dup2(null, target_fd);
        libc::close(null);
        if result < 0 {
            libc::close(saved);
            return Err(io::Error::last_os_error());
        }
        Ok(saved)
    }
}

fn restore(target_fd: i32, saved_fd: i32) {
    unsafe {
        libc::dup2(saved_fd, target_fd);
        libc::close(saved_fd);
    }
}
