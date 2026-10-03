//! Process identity strings: what the daemon compares, byte for byte, with
//! what it reads itself to tell a live registered process from a reused PID.

use std::io;

/// The Windows FILETIME of the Unix epoch (100 ns ticks since 1601).
const UNIX_EPOCH_FILETIME: i128 = 116_444_736_000_000_000;

/// The identity of process `pid`: `proc:PID:starttime:T` on Linux and WSL,
/// `pid:PID:start:NS` on Windows.
pub fn process_identity(pid: u32) -> io::Result<String> {
    os_identity(pid)
}

/// `proc:PID:starttime:T` from the text of `/proc/PID/stat`: T is field 22,
/// index 19 of the whitespace-split text after the last `)` (the command
/// name may hold spaces and parentheses), and must be a positive integer.
pub fn identity_from_stat(pid: u32, stat: &str) -> io::Result<String> {
    let invalid = |why: &str| io::Error::new(io::ErrorKind::InvalidData, format!("/proc/{pid}/stat: {why}"));
    let after = stat.rfind(')').map(|i| &stat[i + 1..]).ok_or_else(|| invalid("no command name terminator"))?;
    let field = after.split_whitespace().nth(19).ok_or_else(|| invalid("no starttime field"))?;
    match field.parse::<u64>() {
        Ok(start) if start > 0 => Ok(format!("proc:{pid}:starttime:{start}")),
        _ => Err(invalid("starttime is not a positive integer")),
    }
}

/// `pid:PID:start:NS` from a process creation FILETIME: NS is the creation
/// time in Unix nanoseconds, `(filetime - 116444736000000000) * 100`.
pub fn identity_from_filetime(pid: u32, creation_filetime: u64) -> String {
    let unix_ns = (creation_filetime as i128 - UNIX_EPOCH_FILETIME) * 100;
    format!("pid:{pid}:start:{unix_ns}")
}

#[cfg(windows)]
fn os_identity(pid: u32) -> io::Result<String> {
    use windows_sys::Win32::Foundation::{CloseHandle, FILETIME};
    use windows_sys::Win32::System::Threading::{
        GetCurrentProcess, GetProcessTimes, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    };
    let current = pid == std::process::id();
    // SAFETY: OpenProcess takes plain values; a null handle is checked below.
    let handle = if current {
        unsafe { GetCurrentProcess() }
    } else {
        unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) }
    };
    if handle.is_null() {
        return Err(io::Error::last_os_error());
    }
    let zero = FILETIME { dwLowDateTime: 0, dwHighDateTime: 0 };
    let (mut created, mut exited, mut kernel, mut user) = (zero, zero, zero, zero);
    // SAFETY: a process handle with query rights and four valid out pointers.
    let ok = unsafe { GetProcessTimes(handle, &mut created, &mut exited, &mut kernel, &mut user) };
    let error = io::Error::last_os_error();
    if !current {
        // SAFETY: the handle came from OpenProcess and is closed once.
        unsafe { CloseHandle(handle) };
    }
    if ok == 0 {
        return Err(error);
    }
    let filetime = ((created.dwHighDateTime as u64) << 32) | created.dwLowDateTime as u64;
    Ok(identity_from_filetime(pid, filetime))
}

#[cfg(not(windows))]
fn os_identity(pid: u32) -> io::Result<String> {
    identity_from_stat(pid, &std::fs::read_to_string(format!("/proc/{pid}/stat"))?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn this_process_has_the_platform_shape() {
        let pid = std::process::id();
        let id = process_identity(pid).unwrap();
        let prefix = if cfg!(windows) { "pid:" } else { "proc:" };
        assert!(id.starts_with(&format!("{prefix}{pid}:")), "{id}");
        assert_eq!(process_identity(pid).unwrap(), id, "stable");
    }

    #[test]
    fn a_missing_process_has_no_identity() {
        assert!(process_identity(u32::MAX - 1).is_err());
    }
}
