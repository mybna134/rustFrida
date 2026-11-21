use std::fs;
use std::io;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProcIdentity {
    pub(crate) id: i32,
    pub(crate) start_time: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LoaderIdentity {
    pub(crate) process: ProcIdentity,
    pub(crate) worker: ProcIdentity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LoaderState {
    Running,
    Exited,
    TargetGone,
    TargetChanged,
    WorkerChanged,
    Unknown,
}

fn parse_stat_identity(stat: &str) -> io::Result<ProcIdentity> {
    let invalid = || io::Error::new(io::ErrorKind::InvalidData, "invalid proc stat identity");
    let id = stat
        .split_once(' ')
        .ok_or_else(invalid)?
        .0
        .parse::<i32>()
        .map_err(|_| invalid())?;
    // comm 可以包含空格和括号；starttime 是末尾右括号之后的第20个字段。
    let fields = stat.rsplit_once(')').ok_or_else(invalid)?.1;
    let start_time = fields
        .split_whitespace()
        .nth(19)
        .ok_or_else(invalid)?
        .parse::<u64>()
        .map_err(|_| invalid())?;
    if id <= 0 {
        return Err(invalid());
    }
    Ok(ProcIdentity { id, start_time })
}

fn read_identity(path: &str) -> io::Result<ProcIdentity> {
    parse_stat_identity(&fs::read_to_string(path)?)
}

pub(crate) fn capture_loader_identity(pid: i32, tid: i32) -> io::Result<LoaderIdentity> {
    if pid <= 0 || tid <= 0 {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid loader pid/tid"));
    }
    let process_path = format!("/proc/{pid}/stat");
    let process = read_identity(&process_path)?;
    let worker = read_identity(&format!("/proc/{pid}/task/{tid}/stat"))?;
    if process.id != pid || worker.id != tid || read_identity(&process_path)? != process {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "loader identity changed"));
    }
    Ok(LoaderIdentity { process, worker })
}

fn classify_loader_state(
    expected: LoaderIdentity,
    process: io::Result<ProcIdentity>,
    worker: io::Result<ProcIdentity>,
) -> LoaderState {
    match process {
        Err(e) if e.kind() == io::ErrorKind::NotFound => return LoaderState::TargetGone,
        Err(_) => return LoaderState::Unknown,
        Ok(actual) if actual != expected.process => return LoaderState::TargetChanged,
        Ok(_) => {}
    }
    match worker {
        Ok(actual) if actual == expected.worker => LoaderState::Running,
        Ok(_) => LoaderState::WorkerChanged,
        Err(e) if e.kind() == io::ErrorKind::NotFound => LoaderState::Exited,
        Err(_) => LoaderState::Unknown,
    }
}

pub(crate) fn inspect_loader(expected: LoaderIdentity) -> LoaderState {
    let pid = expected.process.id;
    let tid = expected.worker.id;
    let worker = read_identity(&format!("/proc/{pid}/task/{tid}/stat"));
    // 在检查线程之后再次确认进程身份，不能把进程退出/换号当成允许远程释放。
    let process = read_identity(&format!("/proc/{pid}/stat"));
    classify_loader_state(expected, process, worker)
}

pub(crate) fn wait_for_loader_exit(expected: LoaderIdentity, deadline: Instant) -> LoaderState {
    loop {
        let state = inspect_loader(expected);
        if state != LoaderState::Running || Instant::now() >= deadline {
            return state;
        }
        std::thread::sleep(
            deadline
                .saturating_duration_since(Instant::now())
                .min(Duration::from_millis(50)),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expected() -> LoaderIdentity {
        LoaderIdentity {
            process: ProcIdentity {
                id: 42,
                start_time: 100,
            },
            worker: ProcIdentity {
                id: 43,
                start_time: 200,
            },
        }
    }

    fn error(kind: io::ErrorKind) -> io::Result<ProcIdentity> {
        Err(io::Error::from(kind))
    }

    #[test]
    fn parses_names_with_spaces_and_parentheses() {
        let mut fields = vec!["0"; 30];
        fields[0] = "S";
        fields[19] = "123456";
        let stat = format!("42 (a worker) with (parentheses)) {}", fields.join(" "));
        assert_eq!(
            parse_stat_identity(&stat).unwrap(),
            ProcIdentity {
                id: 42,
                start_time: 123456
            }
        );
    }

    #[test]
    fn rejects_malformed_stat() {
        for stat in ["", "42 missing", "0 (worker) S", "42 (worker) S 0", "bad (worker) S"] {
            assert!(parse_stat_identity(stat).is_err());
        }
    }

    #[test]
    fn bye_is_not_worker_exit() {
        let id = expected();
        assert_eq!(
            classify_loader_state(id, Ok(id.process), Ok(id.worker)),
            LoaderState::Running
        );
    }

    #[test]
    fn only_missing_worker_in_same_process_is_safe() {
        let id = expected();
        assert_eq!(
            classify_loader_state(id, Ok(id.process), error(io::ErrorKind::NotFound)),
            LoaderState::Exited
        );
    }

    #[test]
    fn permission_failure_is_not_exit() {
        let id = expected();
        assert_eq!(
            classify_loader_state(id, Ok(id.process), error(io::ErrorKind::PermissionDenied)),
            LoaderState::Unknown
        );
        assert_eq!(
            classify_loader_state(
                id,
                error(io::ErrorKind::PermissionDenied),
                error(io::ErrorKind::NotFound)
            ),
            LoaderState::Unknown
        );
    }

    #[test]
    fn reused_process_is_not_safe() {
        let id = expected();
        let changed = ProcIdentity {
            start_time: 101,
            ..id.process
        };
        assert_eq!(
            classify_loader_state(id, Ok(changed), error(io::ErrorKind::NotFound)),
            LoaderState::TargetChanged
        );
    }

    #[test]
    fn reused_worker_is_not_safe() {
        let id = expected();
        let changed = ProcIdentity {
            start_time: 201,
            ..id.worker
        };
        assert_eq!(
            classify_loader_state(id, Ok(id.process), Ok(changed)),
            LoaderState::WorkerChanged
        );
    }

    #[test]
    fn exited_process_needs_no_remote_cleanup() {
        let id = expected();
        assert_eq!(
            classify_loader_state(id, error(io::ErrorKind::NotFound), error(io::ErrorKind::NotFound)),
            LoaderState::TargetGone
        );
    }

    #[test]
    fn malformed_worker_state_is_not_exit() {
        let id = expected();
        assert_eq!(
            classify_loader_state(id, Ok(id.process), error(io::ErrorKind::InvalidData)),
            LoaderState::Unknown
        );
    }

    #[test]
    fn invalid_identifiers_are_rejected() {
        assert!(capture_loader_identity(0, 43).is_err());
        assert!(capture_loader_identity(42, 0).is_err());
    }
}
