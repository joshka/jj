// Copyright 2026 The Jujutsu Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Git object-store garbage collection.
//!
//! This follows the safety model of `git gc --prune=<cutoff>`: a `gc.pid`
//! lock excludes concurrent collectors, and every object reachable from refs,
//! reflogs, worktree HEADs and indexes, or modified after the cutoff, is
//! retained. As with Git, concurrent writers are protected by the grace period
//! rather than by excluding them.

use std::fs;
use std::fs::OpenOptions;
use std::io;
use std::io::Write as _;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use std::time::SystemTime;

use crate::git_backend::GitGcError;

/// Default `gc.reflogExpire`.
const REFLOG_EXPIRE: Duration = Duration::from_secs(90 * 24 * 60 * 60);
/// Default `gc.reflogExpireUnreachable`.
const REFLOG_EXPIRE_UNREACHABLE: Duration = Duration::from_secs(30 * 24 * 60 * 60);

/// Holds Git's `gc.pid` lock for the duration of a collection.
struct GcLock;

struct GcLockGuard {
    path: PathBuf,
}

impl Drop for GcLockGuard {
    fn drop(&mut self) {
        fs::remove_file(&self.path).ok();
    }
}

impl girt::retention::MaintenanceIsolation for GcLock {
    type Guard = GcLockGuard;

    fn acquire(&mut self, repository: &girt::Repository) -> io::Result<Self::Guard> {
        let path = repository.common_dir().join("gc.pid");
        let host = hostname();
        let mut file = match create_lock(&path) {
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists && is_stale(&path, &host) => {
                // Like Git, take over a lock whose collector is gone.
                fs::remove_file(&path)?;
                create_lock(&path)
            }
            result => result,
        }
        .map_err(|err| {
            if err.kind() == io::ErrorKind::AlreadyExists {
                io::Error::new(
                    err.kind(),
                    format!("another gc is running (remove {} if not)", path.display()),
                )
            } else {
                err
            }
        })?;
        writeln!(file, "{} {host}", std::process::id())?;
        Ok(GcLockGuard { path })
    }
}

fn create_lock(path: &Path) -> io::Result<fs::File> {
    OpenOptions::new().write(true).create_new(true).open(path)
}

/// Whether an existing `gc.pid` belongs to a collector that has finished.
///
/// Follows Git: the lock is stale when it is older than 12 hours, or when it
/// names this host and its process no longer exists.
fn is_stale(path: &Path, host: &str) -> bool {
    let age = fs::metadata(path)
        .and_then(|metadata| metadata.modified())
        .ok()
        .and_then(|modified| modified.elapsed().ok());
    if age.is_some_and(|age| age > STALE_LOCK_AGE) {
        return true;
    }
    let Ok(contents) = fs::read_to_string(path) else {
        return false;
    };
    let mut fields = contents.split_whitespace();
    let (Some(pid), lock_host) = (fields.next(), fields.next().unwrap_or_default()) else {
        return false;
    };
    lock_host == host && pid.parse().is_ok_and(|pid| !process_exists(pid))
}

/// Age after which Git treats a `gc.pid` as abandoned.
const STALE_LOCK_AGE: Duration = Duration::from_secs(12 * 60 * 60);

#[cfg(unix)]
fn process_exists(pid: i32) -> bool {
    let Some(pid) = rustix::process::Pid::from_raw(pid) else {
        return false;
    };
    // EPERM means the process exists but belongs to another user.
    !matches!(
        rustix::process::test_kill_process(pid),
        Err(rustix::io::Errno::SRCH)
    )
}

#[cfg(not(unix))]
fn process_exists(_pid: i32) -> bool {
    // Without a liveness check, only the age rule can release a lock.
    true
}

#[cfg(unix)]
fn hostname() -> String {
    rustix::system::uname()
        .nodename()
        .to_string_lossy()
        .into_owned()
}

#[cfg(not(unix))]
fn hostname() -> String {
    std::env::var("COMPUTERNAME").unwrap_or_default()
}

fn unix_seconds(time: SystemTime) -> i128 {
    match time.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => duration.as_secs().into(),
        Err(err) => -i128::from(err.duration().as_secs()),
    }
}

/// Expires old reflog entries and removes unreachable loose objects older than
/// `keep_newer`.
///
/// Unlike `git gc`, this doesn't repack: girt's repack decodes every object and
/// rewrites them without deltas, which makes typical repositories several
/// times larger. Packs are left as they are until girt can repack by reusing
/// existing deltas.
pub(crate) fn collect_garbage(
    repo: &girt::Repository,
    keep_newer: SystemTime,
) -> Result<(), GitGcError> {
    let now = SystemTime::now();
    let policy = girt::retention::RetentionPolicy {
        recent_cutoff: keep_newer,
        reflog_expire_before: Some(unix_seconds(now - REFLOG_EXPIRE)),
        reflog_expire_unreachable_before: Some(unix_seconds(now - REFLOG_EXPIRE_UNREACHABLE)),
        ..girt::retention::RetentionPolicy::trusted()
    };
    let cancel = AtomicBool::new(false);
    repo.expire_reflogs(&mut GcLock, &policy, &cancel)
        .map_err(|err| GitGcError::Maintenance(err.into()))?;
    let pruned = repo
        .prune_unreachable_loose(&mut GcLock, &policy, &cancel)
        .map_err(|err| GitGcError::Maintenance(err.into()))?;
    tracing::info!(pruned = pruned.deleted.len(), "git gc completed");
    Ok(())
}
