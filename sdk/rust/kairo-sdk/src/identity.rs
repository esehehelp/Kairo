//! Process identity strings: what the daemon compares, byte for byte, with
//! what it reads itself to tell a live registered process from a reused PID.

use std::io;

/// The identity of process `pid`: `proc:PID:starttime:T`, from
/// `/proc/PID/stat` (an attempt runs in a Linux container; elsewhere it is
/// an error).
pub fn process_identity(pid: u32) -> io::Result<String> {
    identity_from_stat(pid, &std::fs::read_to_string(format!("/proc/{pid}/stat"))?)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(target_os = "linux")]
    fn this_process_has_the_platform_shape() {
        let pid = std::process::id();
        let id = process_identity(pid).unwrap();
        let prefix = "proc:";
        assert!(id.starts_with(&format!("{prefix}{pid}:")), "{id}");
        assert_eq!(process_identity(pid).unwrap(), id, "stable");
    }

    #[test]
    fn a_missing_process_has_no_identity() {
        assert!(process_identity(u32::MAX - 1).is_err());
    }
}
