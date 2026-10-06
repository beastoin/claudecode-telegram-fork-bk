"""Claude Code worker management, backends, tmux, and sessions.

Manages Claude Code worker lifecycle: tmux sessions, backends (Claude CLI,
Codex), session state, machine registry, and worker health monitoring.

Independently importable — no bridge.py dependency at module level.
Imports infrastructure from core.py (logging, DI seams, config).
Owns all worker domain types (WorkerStateEntry, TmuxSessionDict, etc.).
"""

from __future__ import annotations

# ── Infrastructure from core (no circular dependency) ──────────────────
from core import (
    _subprocess_runner, _clock,
    _log, _log_best_effort,
    _str_field, _int_field, _dict_field, _bool_field,
    _RealSubprocessRunner, _RealClock,
    _LOG_ERROR, _LOG_WARN, _LOG_INFO, _LOG_DEBUG,
    _build_app_context,
    _wd_cfg, _res_cfg, _urlopen,
    _DEFAULT_PORTS, _bridge_url_env, _mounts_env, _node_name,
    _CHECKIN_NOTE_PATH, _LEARNING_REMINDER_PATH,
    SubprocessRunner, Clock, MarkdownToken,
    AppContext, get_app_context,
    VERSION,
    BOT_TOKEN, NODE_NAME, NODE_DIR,
    PORT, BRIDGE_BIND, BRIDGE_URL, BRIDGE_PUBLIC_URL, BRIDGE_SSH_TARGET,
    SESSIONS_DIR, TMUX_PREFIX,
    CLAUDE_DIR, CLAUDE_SETTINGS_FILE,
    TIMEOUT_TMUX_CHECK, TIMEOUT_TMUX_SEND, TIMEOUT_REMOTE_CMD,
    TIMEOUT_FILE_TRANSFER, TIMEOUT_GIT_OP, TIMEOUT_LARGE_TRANSFER,
    TIMEOUT_RSYNC, TIMEOUT_FULL_SYNC,
    TIMEOUT_HTTP_API, TIMEOUT_HTTP_DOWNLOAD, TIMEOUT_HTTP_UPLOAD,
    TIMEOUT_PROCESS_WAIT, TIMEOUT_THREAD_JOIN,
    DELAY_TMUX_SEND, DELAY_PIPE_POLL, DELAY_STARTUP, DELAY_STARTUP_LONG,
    DELAY_RETRY, DELAY_BRIEF, DELAY_SHORT, DELAY_RESPONSE_GAP,
    DELAY_PROCESS_SETTLE, DELAY_CLAUDE_LOAD,
    DEFAULT_BACKEND, DEFAULT_WORKER_BACKEND, PENDING_TIMEOUT,
    FILE_INBOX_ROOT, WORKER_PIPE_ROOT,
    SANDBOX_ENABLED, SANDBOX_IMAGE, SANDBOX_EXTRA_MOUNTS,
    TEAM_DIR,
    MACHINES_CONFIG_FILE,
    WEBHOOK_SECRET,
    WatchdogConfig, ResourceAlertConfig, MediaConfig,
    WATCHDOG_INTERVAL, START_GRACE, THINK_GRACE, TOOL_GAP_GRACE,
    STALE_PENDING, CPU_ACTIVE, CPU_IDLE, IDLE_STREAK_STUCK, ALERT_COOLDOWN,
    RESTART_COOLDOWN,
    DISK_WARN_PCT, DISK_ALERT_PCT, DISK_ALERT_GB, DISK_COOLDOWN,
    CPU_HOG_PCT, CPU_HOG_DURATION_MIN, CPU_HOG_COOLDOWN,
    WORKTREE_THRESHOLD_GB, WORKTREE_COOLDOWN,
    MEM_THRESHOLD_PCT, MEM_THRESHOLD_GB, MEM_COOLDOWN,
    IO_IOWAIT_PCT, IO_COOLDOWN, INFRA_COOLDOWN,
    HOST_DOWN_THRESHOLD,
    PERSISTENCE_NOTE,
    STT_ENDPOINT, STT_TIMEOUT,
    ADMIN_CHAT_ID_ENV, admin_chat_id,
)

import collections
from dataclasses import dataclass, field
import fcntl
import hashlib
import http.client
import os
import json
import mimetypes
import re
import secrets
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import types
import uuid
import urllib.error
import urllib.request
from pathlib import Path
from collections.abc import Iterable, Mapping
from typing import IO, Any, Callable, Iterator, Literal, NamedTuple, Protocol, TypedDict, TYPE_CHECKING, cast, runtime_checkable
from urllib.parse import urlparse


# ── Claudecode domain types (owned by this module) ───────────────────


class ReminderState(TypedDict):
    """Per-worker learning-reminder state."""
    response_count: int
    last_reminder_ts: float
    last_response_ts: float
    reminder_pending: bool


class WorkerStateEntry(NamedTuple):
    """Worker state as tracked by watchdog: (status, reason, since_timestamp)."""
    status: str
    reason: str
    since: float


class ParsedWorkerTarget(NamedTuple):
    """Result of parsing 'name@host' or 'name' worker target."""
    name: str
    host: str | None


class TmuxActivityResult(NamedTuple):
    """Result of reading tmux pane activity."""
    activity: str
    context_pct: str | None
    raw_lines: list[str] | None


class AuthorDetection(NamedTuple):
    """Result of detecting message author from text prefix."""
    author: str
    avatar_html: str
    display_text: str


class TmuxSessionDict(TypedDict, total=False):
    """Shape of a scanned tmux session entry."""
    tmux: str
    backend: str
    host: str           # optional — present only for remote sessions
    protocol: str       # optional — relay protocol version
    callback_url: str   # optional — callback URL for relay workers
    version: str        # optional — bridge version
    activity: str       # optional — current activity
    context_pct: str    # optional — context usage percentage


class WorkerSessionDict(TypedDict, total=False):
    """Shape of a legacy session dict for backward compatibility."""
    backend: str
    tmux: str
    host: str
    callback_url: str
    protocol: str
    version: str


class RegistryWorkerDict(TypedDict, total=False):
    """Raw dict shape of a single worker entry in workers.json."""
    backend: str
    chat_id: int | None
    hire_time: int
    host: str           # optional
    home_host: str | None  # optional — preserved across re-registrations
    home_cwd: str | None   # optional — preserved across re-registrations
    protocol: str       # optional — "http" for callback workers
    callback_url: str   # optional — for callback workers
    version: str        # optional — for callback workers
    tools: dict[str, object]  # optional — for callback workers; shape varies


class RegistryFileDict(TypedDict, total=False):
    """Raw dict shape of the top-level workers.json file."""
    version: int
    workers: dict[str, RegistryWorkerDict]


class RewindTokenEntry(TypedDict):
    """Shape of entries in REWIND_TOKENS."""
    name: str
    expires_at: float


class PrReviewTokenEntry(TypedDict):
    """Shape of entries in PR_REVIEW_TOKENS."""
    pr_num: int
    owner: str
    repo: str
    expires_at: float


class ProcStatsEntry(TypedDict):
    """Per-process stats from ps (used by _ps_stats)."""
    cpu: float
    state: str


class QuestionOption(TypedDict):
    """A single option in an interactive prompt."""
    num: int
    label: str
    selected: bool


class QuestionDetails(TypedDict):
    """Interactive question details extracted from tmux pane output."""
    header: str
    options: list[QuestionOption]
    selected_num: int


class DiskUsageDict(TypedDict, total=False):
    """Disk usage probe result from _check_disk_usage."""
    pct: float
    free_gb: float
    total_gb: float
    ts: float  # added when stored in HostHealthState


class MemUsageDict(TypedDict, total=False):
    """Memory usage probe result from _check_mem_usage."""
    pct: int | float
    used_gb: float
    total_gb: float
    avail_gb: float
    top_procs: list[dict[str, object]]
    ts: float


class IoUsageDict(TypedDict, total=False):
    """IO probe result from _check_io_usage."""
    iowait_pct: float
    read_iops: int
    write_iops: int
    util_pct: float
    ts: float


class WorktreeItemDict(TypedDict):
    """Single worktree entry in worktree usage probe."""
    path: str
    size_gb: float


class WorktreeUsageDict(TypedDict):
    """Worktree usage probe result for a host."""
    total_gb: float
    items: list[WorktreeItemDict]
    ts: float


class CpuHogEntry(TypedDict, total=False):
    """Process entry from _get_cpu_hogs."""
    pid: int
    cpu: float
    etime_min: int
    cmd: str


class HealthSummaryDict(TypedDict, total=False):
    """Per-host health summary returned by HostHealthState.to_health_summary."""
    ssh_down: bool
    ssh_down_since: float | None
    disk: DiskUsageDict | None
    mem: MemUsageDict | None
    io: IoUsageDict | None
    cpu_hogs: list[CpuHogEntry]
    worktrees: WorktreeUsageDict | None


class MachineHealthDict(TypedDict, total=False):
    """Health status for a single machine."""
    status: str
    down_since: float | None
    last_error: str | None
    disk: DiskUsageDict | None
    memory: MemUsageDict | None
    io: IoUsageDict | None


class CodexTranscriptEntry(TypedDict, total=False):
    """A parsed entry from a codex transcript."""
    role: str
    text: str
    timestamp: str


class GitPushStateResult(TypedDict, total=False):
    """Result metadata from _git_push_state."""
    orig_sha: str
    orig_branch: str
    staged_files: list[str]
    stash_sha: str | None


class ProcessRegistry:
    """Tracks background process PIDs, pipe reader threads, and pending locks."""

    def __init__(self) -> None:
        """Initialize adapter process tracking and bridge PID references."""
        self.adapter_pids: dict[str, tuple[subprocess.Popen[str], IO[str] | None]] = {}
        self.adapter_pids_lock: threading.Lock = threading.Lock()
        self.pipe_readers: dict[str, tuple[threading.Thread, threading.Event]] = {}
        self.pipe_readers_lock: threading.Lock = threading.Lock()
        self.pending_locks: dict[str, threading.Lock] = {}
        self.pending_locks_guard: threading.Lock = threading.Lock()


class WorkerWatchdogState:
    """Tracks per-worker health, probe results, restart coordination, and watchdog locks."""

    def __init__(self) -> None:
        # Worker probe state
        """Initialize worker watchdog counters, locks, and health maps."""
        self.worker_states: dict[str, WorkerStateEntry] = {}
        self.last_child_ts: dict[str, float] = {}
        self.last_seen_claude: dict[str, float] = {}
        self.last_hook_ts: dict[str, float] = {}
        self.last_alert_ts: dict[str, float] = {}
        self.alert_msg_ids: dict[str, tuple[int, str]] = {}
        self.idle_streak: dict[str, int] = {}
        self.prev_worker_states: dict[str, str] = {}
        self.consecutive_probe_failures: dict[str, int] = {}
        self.consecutive_good_probes: dict[str, int] = {}
        self.consecutive_bad_probes: dict[str, int] = {}
        self.idle_child_baseline: dict[str, int] = {}
        self.prev_children: dict[str, int] = {}
        self.last_activity_ts: dict[str, float] = {}
        self.worker_cwds: dict[str, str] = {}
        # Restart coordination
        self.recent_restarts: dict[str, float] = {}
        self.restart_in_progress: dict[str, float] = {}
        self.restart_lock: threading.Lock = threading.Lock()
        self.force_restart_pending_cwd: dict[str, bool] = {}
        self.waiting_input_details: dict[str, QuestionDetails] = {}
        # Alert cooldowns
        self.last_resolved_ts: dict[str, float] = {}
        # Global watchdog lock
        self.lock: threading.Lock = threading.Lock()
        # Stop event for clean shutdown
        self.stop_event: threading.Event = threading.Event()

    def reset(self) -> None:
        """Reset all state (useful for testing)."""
        self.__init__()  # type: ignore[misc]

    def clear_worker(self, name: str) -> None:
        """Remove all tracking state for a worker."""
        for store in (
            self.worker_states, self.last_child_ts, self.last_seen_claude,
            self.last_hook_ts, self.last_alert_ts, self.alert_msg_ids,
            self.idle_streak, self.prev_worker_states,
            self.consecutive_probe_failures, self.consecutive_good_probes,
            self.consecutive_bad_probes, self.idle_child_baseline,
            self.prev_children, self.last_activity_ts, self.worker_cwds,
            self.recent_restarts, self.restart_in_progress,
            self.force_restart_pending_cwd, self.waiting_input_details,
            self.last_resolved_ts,
        ):
            store.pop(name, None)


class LearningReminderState:
    """Tracks per-worker learning reminder counters, timers, and persistence."""

    def __init__(self) -> None:
        """Initialize per-worker reminder counters, lock, and idle timer."""
        self.state: dict[str, ReminderState] = {}
        self.lock: threading.Lock = threading.Lock()
        self.idle_scan_timer: threading.Timer | None = None


class HostHealthState:
    """Tracks health metrics for all remote hosts (SSH, disk, CPU, memory, IO, worktrees, Tailscale).

    Thread safety: all reads/writes to mutable dicts must be under watchdog.lock
    (the canonical lock for all watchdog + host_health state).
    """

    def __init__(self) -> None:
        """Initialize per-host health metrics (SSH, disk, memory, IO, CPU, Tailscale)."""
        # SSH connectivity
        self.ssh_failures: dict[str, int] = {}
        self.down: dict[str, bool] = {}
        self.down_since: dict[str, float] = {}
        self.last_error: dict[str, str] = {}
        # Disk
        self.disk_usage: dict[str, DiskUsageDict] = {}
        self.disk_alert_ts: dict[str, float] = {}
        self.disk_alerted: dict[str, str | bool] = {}
        # CPU hogs
        self.cpu_hogs: dict[str, list[CpuHogEntry]] = {}
        self.cpu_hog_alert_ts: dict[str, float] = {}
        # Worktrees
        self.worktree_usage: dict[str, WorktreeUsageDict] = {}
        self.worktree_alert_ts: dict[str, float] = {}
        self.worktree_alerted: dict[str, bool] = {}
        # Memory
        self.mem_usage: dict[str, MemUsageDict] = {}
        self.mem_alert_ts: dict[str, float] = {}
        self.mem_alerted: dict[str, bool] = {}
        # IO
        self.io_usage: dict[str, IoUsageDict] = {}
        self.io_alert_ts: dict[str, float] = {}
        self.io_alerted: dict[str, bool] = {}
        # Infra / Tailscale
        self.tailscale_down: bool = False
        self.tailscale_alert_ts: float = 0.0

    def reset(self) -> None:
        """Reset all state (useful for testing)."""
        self.__init__()  # type: ignore[misc]

    def to_health_summary(self, host: str) -> HealthSummaryDict:
        """Return a typed summary dict for a single host."""
        return HealthSummaryDict(
            ssh_down=self.down.get(host, False),
            ssh_down_since=self.down_since.get(host),
            disk=self.disk_usage.get(host),
            mem=self.mem_usage.get(host),
            io=self.io_usage.get(host),
            cpu_hogs=self.cpu_hogs.get(host, []),
            worktrees=self.worktree_usage.get(host),
        )



# ============================================================
# CORE: Backend Protocol + implementations

def build_claude_start_cmd(resume_id: str = "") -> str:
    """Build the shell command to start a Claude Code interactive session."""
    cmd = ["claude"]
    if resume_id:
        cmd.extend(["--resume", resume_id])
    cmd.append("--dangerously-skip-permissions")
    return " ".join(shlex.quote(part) for part in cmd)



class Backend(Protocol):
    """Backend interface — start, send, and health-check a CLI worker."""
    name: str
    binary: str
    is_interactive: bool

    def start_cmd(self, resume_id: str = "") -> str:
        """Return the shell command to start this CLI in tmux."""
        ...

    def send(self, worker_name: str, tmux_name: str, text: str,
             bridge_url: str, sessions_dir: Path) -> bool:
        """Send a message to the worker. Returns True if sent."""
        ...

    def is_online(self, tmux_name: str) -> bool:
        """Check if worker is alive and ready to receive messages."""
        ...



# ─────────────────────────────────────────────────────────────────────────────
# SSH Teleport helpers (remote worker support)
# ─────────────────────────────────────────────────────────────────────────────

class RemoteCache:
    """Caches for remote host operations (tools, machines, home dirs).

    Thread safety: all reads/writes to mutable dicts must be under self.lock.
    The lock is fine-grained (not held during network I/O).
    """

    def __init__(self) -> None:
        """Initialize caches for SSH host resolution and machine config."""
        self.lock: threading.Lock = threading.Lock()
        self.tools: dict[str, str] = {}     # host:tool -> absolute path
        self.machines: dict[str, "Machine"] | None = None
        self.machines_path: Path | None = None
        self.home_dirs: dict[str, str] = {}  # host -> remote $HOME path



remote_cache = RemoteCache()



def _resolve_remote_tool(tool: str, host: str) -> str:
    """Discover absolute path of a tool on a remote host. Cached per host."""
    key = f"{host}:{tool}"
    with remote_cache.lock:
        cached = remote_cache.tools.get(key)
    if cached:
        return cached
    probe = (
        f'command -v {shlex.quote(tool)} 2>/dev/null || '
        f'for p in /opt/homebrew/bin/{tool} /usr/local/bin/{tool} /usr/bin/{tool} /bin/{tool}; '
        f'do [ -x "$p" ] && echo "$p" && break; done'
    )
    try:
        r = _subprocess_runner.run(
            ["ssh", "-o", "ConnectTimeout=3", host, probe],
            capture_output=True, text=True, timeout=TIMEOUT_TMUX_SEND,
        )
        found = r.stdout.strip()
        if found:
            with remote_cache.lock:
                remote_cache.tools[key] = found
            _log(_LOG_INFO, "bridge", f"discovered {tool} on {host}: {found}")
            return found
    except (subprocess.SubprocessError, OSError) as exc:
        _log(_LOG_DEBUG, "probe:_resolve_remote_tool", f"{type(exc).__name__}: {exc}")
    return tool  # fallback to bare name



def _remote_run(cmd: list[str], host: str | None = None, **kwargs: object) -> subprocess.CompletedProcess[str]:
    """Run a command, optionally on a remote host via SSH.

    When host is None, runs locally. When set, builds a single shell command
    string with proper quoting so the remote shell doesn't eat special chars
    like # (which starts a comment in bash).
    Resolves tool paths automatically on remote hosts (e.g. tmux -> /opt/homebrew/bin/tmux).
    SSH ControlMaster keeps overhead to ~10ms per call.
    """
    if host:
        cmd = list(cmd)
        tool = str(cmd[0])
        if tool in ("tmux", "claude"):
            cmd[0] = _resolve_remote_tool(tool, host)
        remote_cmd = " ".join(shlex.quote(str(a)) for a in cmd)
        _tv = kwargs.get("timeout", 10)
        timeout_val = int(_tv) if isinstance(_tv, (int, float, str)) else 10  # type: ignore[call-overload]
        cmd = ["ssh", "-o", f"ConnectTimeout={min(timeout_val, 5)}", host, remote_cmd]
    # Default timeout: prevent unbounded subprocess hangs that block bridge threads.
    # Hot-path callers should pass explicit shorter timeouts (3s probes, 5s sends).
    kwargs.setdefault("timeout", 10)
    return _subprocess_runner.run(cmd, **kwargs)



# Compatibility shim: _extract_msg_text lives in telegram.py; prefer importing from there.
from telegram import _extract_msg_text as _extract_msg_text  # noqa: F401



# (machines cache moved to remote_cache.machines / remote_cache.machines_path)


def _detect_os_family() -> str:
    """Detect the OS family: 'darwin' on macOS, 'linux' everywhere else."""
    if sys.platform == "darwin":
        return "darwin"
    return "linux"



def _project_slug(cwd: str) -> str:
    """Convert absolute path to Claude Code's project directory slug.

    Claude Code stores sessions at ~/.claude/projects/<slug>/<session-id>.jsonl
    where slug is the CWD with / replaced by -.
    """
    return cwd.replace("/", "-")


# ============================================================
# GIT-BASED TELEPORT SYNC
# ============================================================
# VPS hosts bare repos at ~/git-server/<project>.git.
# Workers push WIP state (via git stash create) to per-worker branches,
# target fetches deltas. ~0-50s vs 600s+ for rsync over Tailscale.

GIT_SERVER_DIR = os.path.expanduser("~/git-server")



def _is_git_repo(cwd: str, host: str | None = None) -> bool:
    """Check if cwd is inside a git repository."""
    try:
        r = _remote_run(
            ["git", "-C", cwd, "rev-parse", "--is-inside-work-tree"],
            host=host, capture_output=True, text=True, timeout=TIMEOUT_REMOTE_CMD)
        return r.returncode == 0
    except (subprocess.SubprocessError, OSError):
        return False



# ─────────────────────────────────────────────────────────────────────────────
# Shared tmux helpers (used by multiple backends)
# ─────────────────────────────────────────────────────────────────────────────

class TmuxSendState:
    """Thread locks and file descriptors for serialized tmux send operations."""

    def __init__(self) -> None:
        """Initialize per-session send locks and flock file descriptors."""
        self.locks: dict[str, threading.Lock] = {}
        self.locks_guard: threading.Lock = threading.Lock()
        self.flock_fds: dict[str, int] = {}



tmux_send = TmuxSendState()



def _get_tmux_send_lock(tmux_name: str) -> threading.Lock:
    """Get or create a lock for a specific tmux session."""
    with tmux_send.locks_guard:
        if tmux_name not in tmux_send.locks:
            tmux_send.locks[tmux_name] = threading.Lock()
        return tmux_send.locks[tmux_name]



def tmux_send_lock_path(tmux_name: str) -> Path:
    """Return the flock file path for a tmux session. Node-namespaced."""
    return Path(f"/tmp/claudecode-telegram/{_node_name}/locks/{tmux_name}.lock")



def _acquire_flock(tmux_name: str) -> int:
    """Acquire a cross-process flock for a tmux session. Returns fd."""
    lock_file = tmux_send_lock_path(tmux_name)
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(lock_file), os.O_CREAT | os.O_RDWR, 0o600)
    import fcntl
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
    except OSError:
        os.close(fd)
        raise
    return fd



def _release_flock(fd: int) -> None:
    """Release a cross-process flock."""
    import fcntl
    fcntl.flock(fd, fcntl.LOCK_UN)
    os.close(fd)



def tmux_exists(tmux_name: str, host: str | None = None, timeout: int = 3) -> bool:
    """Check if tmux session exists (locally or on remote host via SSH)."""
    return _remote_run(
        ["tmux", "has-session", "-t", tmux_name],
        host=host, capture_output=True, timeout=timeout
    ).returncode == 0



def tmux_send_message(tmux_name: str, text: str, host: str | None = None, literal: bool = False) -> bool:
    """Send text + Enter to tmux session via paste-buffer (reliable for long messages).

    Uses tmux load-buffer/paste-buffer instead of send-keys -l to avoid
    character-by-character terminal injection which causes input batching
    on long messages or rapid sends.

    When literal=True, uses send-keys -l instead of paste-buffer.
    This is needed for TUI dialogs (e.g. Claude's OAuth "Paste code here")
    that don't support bracketed paste mode.

    When host is set, uses SSH and pipes text via stdin (no shared filesystem needed).

    Two-layer locking:
    1. Python threading.Lock — serializes sends within this process
    2. flock on a per-session file — serializes sends across processes
       (workers sending via tmux directly use the same lock file)
    """
    lock = _get_tmux_send_lock(tmux_name)
    with lock:
        flock_fd = _acquire_flock(tmux_name)
        try:
            if literal:
                r = _remote_run(
                    ["tmux", "send-keys", "-t", tmux_name, "-l", text],
                    host=host, capture_output=True, timeout=TIMEOUT_TMUX_SEND,
                )
                if r.returncode != 0:
                    return False
                _clock.sleep(DELAY_RETRY)
                r = _remote_run(["tmux", "send-keys", "-t", tmux_name, "Enter"], host=host, timeout=TIMEOUT_TMUX_SEND)
                return r.returncode == 0

            buf_name = f"msg-{uuid.uuid4().hex[:8]}"

            if host:
                # Remote: pipe text via stdin to avoid shared filesystem
                r = _remote_run(
                    ["tmux", "load-buffer", "-b", buf_name, "-"],
                    host=host, input=text.encode(), capture_output=True, timeout=TIMEOUT_TMUX_SEND,
                )
            else:
                # Local: write to temp file for tmux load-buffer
                fd, tmpfile = tempfile.mkstemp(suffix=".msg", prefix="tmux-send-")
                try:
                    try:
                        os.write(fd, text.encode())
                    finally:
                        os.close(fd)
                    r = _subprocess_runner.run(
                        ["tmux", "load-buffer", "-b", buf_name, tmpfile],
                        capture_output=True, timeout=TIMEOUT_TMUX_SEND,
                    )
                finally:
                    try:
                        os.unlink(tmpfile)
                    except OSError as exc:
                        _log(_LOG_DEBUG, "io:unknown", f"{type(exc).__name__}: {exc}")

            if r.returncode != 0:
                return False
            # Paste buffer into the target pane with proper bracketed paste
            # -p: send bracketed paste control codes (\e[200~ ... \e[201~)
            #     so TUI apps (Claude Code) know exactly where paste ends.
            #     Without -p, Enter sent after paste can be swallowed into
            #     the TUI's time-based paste detection window.
            # -r: preserve LF as LF (don't convert to CR). Keeps multi-line
            #     text as multi-line input, not line-by-line Enter presses.
            # -d: delete buffer after pasting
            r = _remote_run(
                ["tmux", "paste-buffer", "-p", "-r", "-t", tmux_name, "-b", buf_name, "-d"],
                host=host, capture_output=True, timeout=TIMEOUT_TMUX_SEND,
            )
            if r.returncode != 0:
                return False
            # Delay after paste: TUI needs time to process paste-end marker
            # and re-render. At low context (1%), Claude Code TUI can take
            # 300-1000ms to render pasted text. Enter sent before render
            # completes hits an empty prompt and the message is silently lost.
            # 50ms → 150ms → 1s: increased after observing silent message
            # loss on prod sessions with heavy context load.
            _clock.sleep(DELAY_STARTUP)
            # Send Enter to submit the pasted text
            r = _remote_run(["tmux", "send-keys", "-t", tmux_name, "Enter"], host=host, timeout=TIMEOUT_TMUX_SEND)
            return r.returncode == 0
        finally:
            _release_flock(flock_fd)



def get_pane_command(tmux_name: str, host: str | None = None) -> str:
    """Get the current command running in tmux pane."""
    result = _remote_run(
        ["tmux", "display-message", "-t", tmux_name, "-p", "#{pane_current_command}"],
        host=host, capture_output=True, text=True, timeout=TIMEOUT_TMUX_CHECK
    )
    return result.stdout.strip() if result.returncode == 0 else ""



def is_process_running(tmux_name: str, process_name: str, host: str | None = None) -> bool:
    """Check if a process is running in tmux session."""
    cmd = get_pane_command(tmux_name, host=host)
    if process_name.lower() in cmd.lower():
        return True

    result = _remote_run(
        ["tmux", "display-message", "-t", tmux_name, "-p", "#{pane_pid}"],
        host=host, capture_output=True, text=True, timeout=TIMEOUT_TMUX_CHECK
    )
    if result.returncode != 0:
        return False

    pane_pid = result.stdout.strip()
    if not pane_pid:
        return False

    result = _remote_run(
        ["pgrep", "-P", pane_pid, process_name],
        host=host, capture_output=True, timeout=TIMEOUT_TMUX_CHECK
    )
    return result.returncode == 0



def tmux_send_escape(tmux_name: str, host: str | None = None) -> None:
    """Send an escape key sequence to a tmux pane."""
    _remote_run(["tmux", "send-keys", "-t", tmux_name, "Escape"], host=host, timeout=TIMEOUT_TMUX_SEND)



def _tmux_pane_pids(host: str | None = None) -> dict[str, str]:
    """Return a map of tmux session_name -> pane_pid for all panes."""
    try:
        result = _remote_run(
            ["tmux", "list-panes", "-a", "-F", "#{session_name} #{pane_pid}"],
            host=host, capture_output=True, text=True, timeout=TIMEOUT_TMUX_SEND
        )
    except (subprocess.SubprocessError, OSError):
        return {}

    if result.returncode != 0:
        return {}

    pane_map = {}
    for line in result.stdout.splitlines():
        parts = line.strip().split()
        if len(parts) < 2:
            continue
        session_name, pane_pid = parts[0], parts[1]
        if pane_pid.isdigit():
            pane_map[session_name] = pane_pid
    return pane_map



class ClaudeBackend:
    """Claude Code CLI - interactive mode with hook for responses."""
    name = "claude"
    binary = "claude"
    is_interactive = True

    def start_cmd(self, resume_id: str = "") -> str:
        """Start cmd."""
        return build_claude_start_cmd(resume_id)

    def send(self, worker_name: str, tmux_name: str, text: str,
             bridge_url: str, sessions_dir: Path) -> bool:
        """Send."""
        import bridge
        host = bridge.get_worker_host(worker_name)
        try:
            if not tmux_exists(tmux_name, host=host, timeout=TIMEOUT_TMUX_CHECK):
                return False
        except (subprocess.SubprocessError, OSError):
            if not host:
                return False
            # Remote probe failure — don't block send, attempt delivery anyway
        # Claude's OAuth login dialog doesn't support bracketed paste.
        # Detect login state and use literal send-keys instead.
        literal = False
        try:
            r = _remote_run(
                ["tmux", "capture-pane", "-t", tmux_name, "-p"],
                host=host, capture_output=True, text=True, timeout=TIMEOUT_TMUX_SEND,
            )
            if r.returncode == 0 and "Paste code here" in r.stdout:
                literal = True
        except (subprocess.SubprocessError, OSError) as exc:
            _log(_LOG_DEBUG, "probe:send", f"{type(exc).__name__}: {exc}")
        return tmux_send_message(tmux_name, text, host=host, literal=literal)

    def is_online(self, tmux_name: str) -> bool:
        # Note: is_online doesn't have worker_name, so can't look up host.
        # For remote workers, the watchdog uses different detection.
        """Is online."""
        if not tmux_exists(tmux_name):
            return False
        return is_process_running(tmux_name, "claude")



# ─────────────────────────────────────────────────────────────────────────────
# Codex adapter (was hooks/codex-tmux-adapter.py — merged into bridge)
# ─────────────────────────────────────────────────────────────────────────────


def _codex_session_id_path(worker_name: str, sessions_dir: Path) -> Path:
    """Path to the file storing codex session ID."""
    return sessions_dir / worker_name / "codex_session_id"



def _codex_load_session_id(worker_name: str, sessions_dir: Path) -> str:
    """Load saved codex session ID for worker. Returns '' if none."""
    p = _codex_session_id_path(worker_name, sessions_dir)
    if p.exists():
        return p.read_text(encoding="utf-8").strip()
    return ""



def _codex_save_session_id(worker_name: str, sessions_dir: Path, session_id: str) -> None:
    """Atomically save codex session ID for worker (tmp + rename)."""
    target = _codex_session_id_path(worker_name, sessions_dir)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd: int = -1
    try:
        fd, tmp_path = tempfile.mkstemp(dir=str(target.parent), prefix=".session_id_")
        os.write(fd, session_id.encode("utf-8"))
        os.close(fd)
        fd = -1
        os.replace(tmp_path, str(target))
    except BaseException:
        if fd >= 0:
            os.close(fd)
        raise



def _codex_parse_jsonl(output: str) -> tuple[str, str]:
    """Parse JSONL output from codex exec --json.

    Returns (response_text, thread_id).
    """
    response_parts: list[str] = []
    thread_id: str = ""

    for line in output.strip().split("\n"):
        if not line.strip():
            continue
        try:
            event: dict[str, object] = json.loads(line)
        except json.JSONDecodeError:
            continue

        event_type = _str_field(event, "type")
        if event_type == "thread.started":
            thread_id = _str_field(event, "thread_id")
        elif event_type == "item.completed":
            item = _dict_field(event, "item")
            if _str_field(item, "type") == "agent_message":
                text = _str_field(item, "text")
                if text:
                    response_parts.append(text)

    return "\n".join(response_parts).strip(), thread_id



def _codex_run(message: str, session_id: str = "", workdir: str = "") -> tuple[str, str, int]:
    """Run codex exec and return (response, session_id, returncode)."""
    cmd: list[str] = ["codex", "exec", "--json", "--yolo"]
    if workdir:
        cmd.extend(["-C", workdir])
    if session_id and not session_id.startswith("-"):
        cmd.extend(["resume", session_id, "-"])
    else:
        cmd.append("-")

    try:
        result = subprocess.run(cmd, input=message, capture_output=True, text=True)
        response, new_session_id = _codex_parse_jsonl(result.stdout)
        if result.returncode != 0 and not response:
            stderr = (result.stderr or "").strip()
            response = stderr or "Codex exec failed."
        return response, new_session_id or session_id, result.returncode
    except (OSError, subprocess.SubprocessError) as e:
        return f"Error: {e}", session_id, 1



def _codex_send_to_bridge(session_name: str, text: str, bridge_url: str) -> bool:
    """Send codex response to bridge (raw text, no escaping)."""
    try:
        payload: dict[str, str | bool] = {
            "session": session_name, "text": text,
            "source": session_name, "backend": "codex", "escape": True,
        }
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            f"{bridge_url}/response", data=data,
            headers={"Content-Type": "application/json"},
        )
        with _urlopen(req, timeout=5) as r:
            return r.status == 200
    except (urllib.error.URLError, OSError) as e:
        _log(_LOG_WARN, "codex", f"Failed to send to bridge: {e}")
        return False



def _codex_adapter_thread(worker_name: str, text: str,
                          bridge_url: str, sessions_dir: Path) -> None:
    """Thread target: run codex for a worker and forward response to bridge."""
    try:
        session_id = _codex_load_session_id(worker_name, sessions_dir)
        response, new_session_id, _rc = _codex_run(text, session_id)

        if new_session_id:
            _codex_save_session_id(worker_name, sessions_dir, new_session_id)

        if response:
            _codex_send_to_bridge(worker_name, response, bridge_url)
    except Exception as e:
        _log(_LOG_ERROR, "codex", f"Adapter thread for '{worker_name}' failed: {e}")



def _codex_adapter_remote(worker_name: str, text: str,
                          bridge_url: str, sessions_dir: Path, host: str) -> None:
    """Thread target: run codex on a remote host via SSH, parse output locally."""
    try:
        # Load session_id from remote
        import bridge
        remote_sessions = bridge._remap_sessions_dir(host)
        sid_file = f"{remote_sessions}/{worker_name}/codex_session_id"
        session_id = ""
        try:
            r = subprocess.run(
                ["ssh", "-o", "ConnectTimeout=5", host, "cat", sid_file],
                capture_output=True, text=True, timeout=10,
            )
            if r.returncode == 0:
                session_id = r.stdout.strip()
        except (OSError, subprocess.TimeoutExpired):
            pass

        # Build codex command
        cmd_parts = ["codex", "exec", "--json", "--yolo"]
        if session_id and not session_id.startswith("-"):
            cmd_parts.extend(["resume", session_id, "-"])
        else:
            cmd_parts.append("-")

        # SSH + run codex with message on stdin
        ssh_cmd = ["ssh", "-o", "ConnectTimeout=5", host] + cmd_parts
        result = subprocess.run(ssh_cmd, input=text, capture_output=True, text=True)
        response, new_session_id = _codex_parse_jsonl(result.stdout)

        if result.returncode != 0 and not response:
            response = (result.stderr or "").strip() or "Codex exec failed."

        # Save session_id on remote
        if new_session_id:
            try:
                subprocess.run(
                    ["ssh", "-o", "ConnectTimeout=5", host,
                     "mkdir", "-p", f"{remote_sessions}/{worker_name}",
                     "&&", "echo", shlex.quote(new_session_id), ">", sid_file],
                    timeout=10, capture_output=True,
                )
            except (OSError, subprocess.TimeoutExpired):
                pass

        # POST response to bridge (use public URL for remote)
        if response:
            target_url = BRIDGE_PUBLIC_URL or bridge_url
            _codex_send_to_bridge(worker_name, response, target_url)

    except Exception as e:
        _log(_LOG_ERROR, "codex", f"Remote adapter for '{worker_name}' on {host} failed: {e}")



class CodexBackend:
    """OpenAI Codex CLI - non-interactive mode."""
    name = "codex"
    binary = "codex"
    is_interactive = False
    is_exec = True

    def start_cmd(self, resume_id: str = "") -> str:
        """Start cmd."""
        return "echo 'Codex worker ready (non-interactive)'"

    def send(self, worker_name: str, tmux_name: str, text: str,
             bridge_url: str, sessions_dir: Path) -> bool:
        """Send message to codex worker — runs in a background thread."""
        import bridge
        host = bridge.get_worker_host(worker_name)
        if host:
            t = threading.Thread(
                target=_codex_adapter_remote,
                args=(worker_name, text, bridge_url, sessions_dir, host),
                daemon=True,
            )
        else:
            t = threading.Thread(
                target=_codex_adapter_thread,
                args=(worker_name, text, bridge_url, sessions_dir),
                daemon=True,
            )
        t.start()
        return True

    def is_online(self, tmux_name: str) -> bool:
        """Is online."""
        return tmux_exists(tmux_name)



BACKENDS: dict[str, Backend] = {
    "claude": ClaudeBackend(),
    "codex": CodexBackend(),
}



# _spawn_adapter and _spawn_adapter_remote removed — codex adapter is now
# inline in bridge.py (see _codex_adapter_thread / _codex_adapter_remote above CodexBackend).



def _find_codex_transcript(worker_name: str, host: str | None = None) -> str | None:
    """Find the codex native JSONL transcript file for a worker.

    Uses codex_session_id to locate the file under ~/.codex/sessions/.
    Returns the file path or None if not found.
    """
    sid_file = SESSIONS_DIR / worker_name / "codex_session_id"
    if not sid_file.exists():
        return None
    session_id = sid_file.read_text().strip()
    if not session_id:
        return None

    home = os.path.expanduser("~")
    if host:
        home = _get_remote_home(host) or home
    codex_dir = os.path.join(home, ".codex", "sessions")

    if host:
        try:
            r = _remote_run(
                ["find", codex_dir, "-name", f"*{session_id}*", "-name", "*.jsonl"],
                host=host, capture_output=True, text=True, timeout=TIMEOUT_REMOTE_CMD)
            if r.returncode == 0 and r.stdout.strip():
                return r.stdout.strip().split("\n")[0]
        except (subprocess.SubprocessError, OSError) as exc:
            _log(_LOG_DEBUG, "probe:_find_codex_transcript", f"{type(exc).__name__}: {exc}")
        return None

    import glob
    matches = glob.glob(os.path.join(codex_dir, "**", f"*{session_id}*"), recursive=True)
    jsonl_matches = [m for m in matches if m.endswith(".jsonl")]
    if jsonl_matches:
        return max(jsonl_matches, key=os.path.getmtime)
    return None



def get_backend(name: str) -> Backend:
    """Look up a backend class by name."""
    return cast(Backend, BACKENDS.get(name, BACKENDS[DEFAULT_BACKEND]))



def is_valid_backend(name: str) -> bool:
    """Check whether a backend name is registered."""
    return name in BACKENDS



def list_backends() -> list[str]:
    """Return list of all registered backend names."""
    return list(BACKENDS.keys())



def _which_binary(binary: str) -> str | None:
    """Find binary in PATH, including common user install locations.

    The bridge may run with a minimal PATH (e.g. via env -i), missing
    ~/.local/bin or ~/bin where claude/codex are typically installed.
    """
    found = shutil.which(binary)
    if found:
        return found
    home = os.environ.get("HOME", "")
    if home:
        for extra_dir in [os.path.join(home, ".local", "bin"), os.path.join(home, "bin")]:
            candidate = os.path.join(extra_dir, binary)
            if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
                return candidate
    return None



def is_claude_running(tmux_name: str, host: str | None = None) -> bool:
    """Check if an interactive backend process is running in the given tmux pane."""
    return is_process_running(tmux_name, "claude", host=host)



# Singletons: instantiated here, shared with bridge.py via _BridgeModule propagation.
processes = ProcessRegistry()
watchdog = WorkerWatchdogState()
learning_reminders = LearningReminderState()
host_health = HostHealthState()
# worker_manager is set by bridge.py after WorkerManager class loads (defined after import).
# Stub None so _BridgeModule.__setattr__ propagation can fill it.
worker_manager = None


# Learning reminders: periodic self-learning nudges per worker
# Two triggers: response count threshold, and idle timeout (checked by timer).
# Anti-annoyance: after any reminder fires, all triggers suppressed until worker responds.
# State is persisted to disk so bridge restarts don't reset progress.
LEARNING_REMINDER_RESPONSE_THRESHOLD = 15  # fire after N worker responses

LEARNING_REMINDER_IDLE_HOURS = 6  # fire if no worker response in N hours



_LEARNING_REMINDER_TEXT = (
    "system: Self-Learning Protocol reminder — time to check your learnings.\n\n"
    "You own your learning. Do not wait for approval to update your playbook.\n\n"
    "**What to capture:**\n"
    "Decisions that surprised you, corrections from manager or teammates, "
    "patterns you will use again, mistakes you will not repeat, "
    "tool/API behaviors that were not obvious.\n\n"
    "**What NOT to capture:**\n"
    "Routine task notes, things already in the code or git history, "
    "one-off fixes with no reuse value, debugging steps that only apply to a specific bug.\n\n"
    "**Format:**\n"
    'Write every rule as: "When X, do Y, because Z." '
    'The "because Z" is the most important part — without it the rule has no context '
    "and cannot be judged in edge cases.\n\n"
    "**Cap:**\n"
    "Maximum 20 active rules. When you hit 20, replace your weakest rule. "
    "A tight playbook of battle-tested rules beats a long list nobody reads.\n\n"
    "**Where to write:**\n"
    "~/team/{name}/playbook.md for rules specific to your role/tools/project.\n"
    "~/team/learnings.md for lessons that help other workers (cross-team value). "
    "Include date, description, tags, and your name.\n"
    "Do NOT duplicate between personal playbook and shared learnings — pick one home.\n\n"
    "**Steps:**\n"
    "1. Reflect — scan your recent work. Did you hit a surprise, get corrected, "
    "or discover a reusable pattern?\n"
    "2. If yes — read your ~/team/{name}/playbook.md, check if the lesson already exists. "
    "Update an existing rule or add a new one.\n"
    "3. If cross-team value — add a one-liner to ~/team/learnings.md.\n"
    "4. If nothing worth keeping — carry on. Not every session produces a learning.\n"
    "5. Clean — if over 20 rules, archive your weakest one.\n\n"
    "**Quality check:**\n"
    'Good: "When backend returns 500 on auth-token, check if Firebase emulator is running first, '
    'because the error message says connection refused which misleads you into checking network config."\n'
    'Bad: "Fixed auth-token bug." (no When/because, no reuse value, will rot)'
)



# Claude Code stores transcripts at ~/.claude/projects/<slug>/<uuid>.jsonl.
# Overridable in tests.
CLAUDE_PROJECTS_DIR = Path(os.path.expanduser("~/.claude/projects"))



# Token stores — still use raw dicts internally for backward compat,
# but the dataclasses above define the canonical shape.
REWIND_TOKENS: dict[str, RewindTokenEntry] = {}

PR_REVIEW_TOKENS: dict[str, PrReviewTokenEntry] = {}



# ─────────────────────────────────────────────────────────────────────────────
# Persistent Worker Registry
# ─────────────────────────────────────────────────────────────────────────────

WORKER_REGISTRY_FILE = NODE_DIR / "workers.json"



# Reserved names that cannot be used as worker names (would clash with commands)
RESERVED_NAMES = {
    # Bridge commands
    "team", "focus", "restart", "settings", "hire", "end",
    # Special
    "all", "cancel", "start", "help",
}



# ============================================================
# INTER-WORKER PIPES
# ============================================================

# ─────────────────────────────────────────────────────────────────────────────
# Worker Pipe Functions (inter-worker communication)
# ─────────────────────────────────────────────────────────────────────────────

def get_worker_pipe_path(name: str) -> Path:
    """Get the named pipe path for a worker.

    Path: /tmp/claudecode-telegram/<node>/<worker>/in.pipe
    """
    return WORKER_PIPE_ROOT / name / "in.pipe"



def ensure_worker_pipe(name: str) -> Path:
    """Create the named pipe for a worker if it doesn't exist.

    Creates: /tmp/claudecode-telegram/<node>/<worker>/in.pipe
    Also starts a reader thread to forward messages to the worker.
    """
    pipe_path = get_worker_pipe_path(name)
    pipe_dir = pipe_path.parent

    # Create directory with secure permissions
    pipe_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    pipe_dir.chmod(0o700)

    # Create FIFO (named pipe) if it doesn't exist
    if not pipe_path.exists():
        os.mkfifo(str(pipe_path), mode=0o600)
        _log(_LOG_INFO, "pipe", f"Created worker pipe: {pipe_path}")

    # Start the pipe reader thread to forward messages to worker
    start_pipe_reader(name)

    return pipe_path



def cleanup_worker_pipe(name: str) -> None:
    """Remove the named pipe for a worker."""
    # Stop the pipe reader thread first
    stop_pipe_reader(name)

    pipe_path = get_worker_pipe_path(name)

    if pipe_path.exists():
        try:
            pipe_path.unlink()
            _log(_LOG_INFO, "pipe", f"Removed worker pipe: {pipe_path}")
        except OSError as e:
            _log(_LOG_WARN, "worker", f"Failed to remove worker pipe {pipe_path}: {e}")

    # Also try to remove parent directory if empty
    pipe_dir = pipe_path.parent
    if pipe_dir.exists():
        try:
            pipe_dir.rmdir()
        except OSError:
            pass  # intentional no-op: directory not empty is expected (other workers' pipes)



# ─────────────────────────────────────────────────────────────────────────────
# Pipe Reader Threads (for inter-worker communication)
# ─────────────────────────────────────────────────────────────────────────────

# Dict to track pipe reader threads: name -> (thread, stop_event)
# (pipe reader threads moved to processes.pipe_readers)


def pipe_reader_loop(name: str, stop_event: threading.Event) -> None:
    """Background thread that reads messages from a worker's input pipe.

    When another worker writes to this worker's pipe:
      echo "message" > /tmp/claudecode-telegram/<node>/bob/in.pipe

    This thread reads the message and forwards it to the worker's backend.

    The reader uses blocking open() - this means the thread will block until
    a writer opens the pipe. This is correct behavior for FIFOs. When the
    writer closes, we get EOF, close our end, and re-open to wait for the
    next writer.
    """
    pipe_path = get_worker_pipe_path(name)
    _log(_LOG_INFO, "pipe", f"Pipe reader started for worker '{name}' at {pipe_path}")

    while not stop_event.is_set():
        try:
            # Check if we should stop before blocking on open
            if stop_event.is_set():
                break

            # Open pipe for reading (blocks until a writer connects)
            # Use regular open() which blocks - this is the correct way to read FIFOs
            with open(str(pipe_path), 'r') as pipe:
                # Read until EOF (writer closes their end)
                while not stop_event.is_set():
                    line = pipe.readline()
                    if not line:
                        # EOF - writer closed, break to re-open
                        break

                    message = line.strip()
                    if message:
                        _log(_LOG_INFO, "pipe", f"Pipe message for '{name}': {message[:100]}{'...' if len(message) > 100 else ''}")
                        # Forward to worker using backend routing
                        try:
                            _forward_pipe_message(name, message)
                        except (OSError, ValueError) as e:
                            _log(_LOG_ERROR, "bridge", f"Error forwarding pipe message to '{name}': {e}")

        except FileNotFoundError:
            # Pipe was removed, stop the reader
            _log(_LOG_WARN, "pipe", f"Pipe for '{name}' no longer exists, stopping reader")
            break
        except OSError as e:
            if stop_event.is_set():
                break
            _log(_LOG_ERROR, "bridge", f"Pipe reader error for '{name}': {e}")
            # Wait a bit before retrying
            stop_event.wait(0.5)

    # Clean up registry so start_pipe_reader can restart if needed
    with processes.pipe_readers_lock:
        if name in processes.pipe_readers:
            processes.pipe_readers.pop(name, None)
    _log(_LOG_INFO, "pipe", f"Pipe reader stopped for worker '{name}'")



def _forward_pipe_message(name: str, message: str) -> None:
    """Forward a message from the pipe to the worker's session.

    Uses backend routing for tmux or non-interactive workers.
    """
    import bridge
    if not bridge.worker_manager.send(name, message):
        _log(_LOG_WARN, "worker", f"Warning: Cannot forward pipe message to '{name}' - worker not found")



def start_pipe_reader(name: str) -> None:
    """Start a background thread to read from the worker's input pipe."""
    with processes.pipe_readers_lock:
        if name in processes.pipe_readers:
            thread, _stop = processes.pipe_readers[name]
            if thread.is_alive():
                # Already running
                return
            # Thread crashed or exited — clean up stale entry and restart
            _log(_LOG_WARN, "pipe", f"Pipe reader thread for '{name}' is dead, restarting")
            processes.pipe_readers.pop(name, None)

    pipe_path = get_worker_pipe_path(name)
    if not pipe_path.exists():
        _log(_LOG_WARN, "bridge", f"Cannot start pipe reader: pipe does not exist for '{name}'")
        return

    stop_event = threading.Event()
    thread = threading.Thread(
        target=pipe_reader_loop,
        args=(name, stop_event),
        daemon=True,
        name=f"pipe-reader-{name}"
    )
    with processes.pipe_readers_lock:
        processes.pipe_readers[name] = (thread, stop_event)
    thread.start()
    _log(_LOG_INFO, "pipe", f"Started pipe reader thread for '{name}'")



def stop_pipe_reader(name: str) -> None:
    """Stop the pipe reader thread for a worker."""
    with processes.pipe_readers_lock:
        if name not in processes.pipe_readers:
            return
        thread, stop_event = processes.pipe_readers.pop(name)
    stop_event.set()

    # Write a dummy byte to unblock the reader if it's waiting
    pipe_path = get_worker_pipe_path(name)
    if pipe_path.exists():
        try:
            # Open in non-blocking write mode to unblock reader
            fd = os.open(str(pipe_path), os.O_WRONLY | os.O_NONBLOCK)
            try:
                os.write(fd, b"\n")
            finally:
                os.close(fd)
        except OSError:
            pass  # intentional no-op: pipe may already be closed by reader

    # Wait for thread to finish (with timeout)
    thread.join(timeout=TIMEOUT_THREAD_JOIN)
    if thread.is_alive():
        _log(_LOG_WARN, "bridge", f"Warning: pipe reader thread for '{name}' did not stop gracefully")



# ============================================================
# TMUX SESSION MANAGEMENT
# ============================================================

# ─────────────────────────────────────────────────────────────────────────────
# Session Management
# ─────────────────────────────────────────────────────────────────────────────

def get_session_dir(name: str) -> Path:
    """Get per-session directory path."""
    return SESSIONS_DIR / name



def ensure_session_dir(name: str) -> Path:
    """Create session directory if needed with secure permissions (0o700)."""
    session_dir = get_session_dir(name)
    session_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    # Ensure parent directories also have secure permissions
    SESSIONS_DIR.chmod(0o700)
    session_dir.chmod(0o700)
    return session_dir



def get_chat_id_file(name: str) -> Path:
    """Return the path to a worker's chat ID file."""
    return get_session_dir(name) / "chat_id"



def _scan_latest_session_id(cwd: str, host: str | None = None) -> str:
    """Return the UUID of the most-recently-modified JSONL in <slug>/ on `host`.

    Source of truth for the "current" session Claude Code is writing to.
    host=None → scan local filesystem. host set → SSH + `ls -1t`.
    Returns "" if the slug dir is missing/empty or the scan errors out.
    """
    if not cwd:
        return ""
    slug = _project_slug(cwd)
    if host:
        try:
            cmd = [
                "bash", "-c",
                f'ls -1t "$HOME/.claude/projects/{slug}"/*.jsonl 2>/dev/null | head -1',
            ]
            r = _remote_run(cmd, host=host, capture_output=True,
                            text=True, timeout=TIMEOUT_REMOTE_CMD)
            if r.returncode != 0:
                return ""
            path = (r.stdout or "").strip()
            if not path:
                return ""
            return os.path.basename(path).removesuffix(".jsonl")
        except (subprocess.SubprocessError, OSError):
            return ""
    # Local scan
    slug_dir = CLAUDE_PROJECTS_DIR / slug
    if not slug_dir.is_dir():
        return ""
    try:
        jsonls = [p for p in slug_dir.iterdir()
                  if p.is_file() and p.suffix == ".jsonl"]
    except OSError:
        return ""
    if not jsonls:
        return ""
    latest = max(jsonls, key=lambda p: p.stat().st_mtime)
    return latest.stem



def _log_session_event(name: str, session_id: str, cwd: str, event: str) -> None:
    """Append a session event to the worker's audit log (best effort)."""
    if not session_id:
        return
    try:
        session_dir = ensure_session_dir(name)
        history_file = session_dir / "session_history.jsonl"
        entry = json.dumps({
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(_clock.time())),
            "session_id": session_id,
            "cwd": cwd or "",
            "event": event,
        })
        with open(history_file, "a") as fh:
            fh.write(entry + "\n")
        history_file.chmod(0o600)
    except OSError as exc:
        _log(_LOG_DEBUG, "io:_log_session_event", f"{type(exc).__name__}: {exc}")



def get_session_history(name: str, event: str | None = None) -> list[dict[str, object]]:
    """Read the session audit log for a worker. Optional event filter."""
    f = get_session_dir(name) / "session_history.jsonl"
    if not f.exists():
        return []
    entries = []
    for line in f.read_text().strip().splitlines():
        if not line:
            continue
        try:
            e = cast(dict[str, object], json.loads(line))  # shape varies per event type
            if event and e.get("event") != event:
                continue
            entries.append(e)
        except json.JSONDecodeError:
            continue
    return entries



# Default path for Claude Code's workspace trust config.
_CLAUDE_JSON_PATH = Path.home() / ".claude.json"



def _ensure_workspace_trusted(
    cwd: str,
    config_path: Path | None = None,
) -> None:
    """Add *cwd* to Claude Code's trusted-workspace list (best effort).

    Claude Code stores workspace trust in ``~/.claude.json`` under
    ``projects.<path>.hasTrustDialogAccepted``.  When a worker restarts
    in a directory that hasn't been trusted yet, Claude shows an
    interactive "trust this folder?" prompt that blocks non-interactive
    sessions.  This function pre-trusts the directory so the prompt
    never appears.

    Uses file locking to prevent concurrent writes from corrupting the
    JSON (e.g., two workers being hired/teleported simultaneously).

    Skips the write if the directory is already trusted.
    """
    if not cwd:
        return
    target = config_path or _CLAUDE_JSON_PATH
    try:
        import fcntl
        lock_path = target.with_suffix(".lock")
        with open(lock_path, "w") as lock_fd:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            try:
                if target.exists():
                    data = cast(dict[str, object], json.loads(target.read_text()))  # body: {projects}
                else:
                    data = {}
                _projects_raw = data.setdefault("projects", {})
                projects = _projects_raw if isinstance(_projects_raw, dict) else {}
                _entry_raw = projects.get(cwd, {})
                entry = _entry_raw if isinstance(_entry_raw, dict) else {}
                if entry.get("hasTrustDialogAccepted") is True:
                    return  # already trusted — skip rewrite
                projects[cwd] = {**entry, "hasTrustDialogAccepted": True}
                _tmp = target.with_suffix('.tmp')
                _tmp.write_text(json.dumps(data, indent=2))
                os.replace(str(_tmp), str(target))
                _log(_LOG_INFO, "trust", f"pre-trusted workspace: {cwd}")
            finally:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
    except (OSError, json.JSONDecodeError) as exc:
        _log(_LOG_WARN, "trust", f"could not pre-trust {cwd}: {exc}")



# Compatibility shim: _build_cwd_change_notice lives in telegram.py; prefer importing from there.
from telegram import _build_cwd_change_notice as _build_cwd_change_notice  # noqa: F401



def _cache_session_id(name: str, sid: str) -> None:
    """Write session_id + its CWD to local cache file (best effort, 0o600).

    The file stores two lines:
        line 1: session UUID
        line 2: CWD path the session was created in

    get_claude_session_id() validates that line 2 matches the worker's
    current CWD. If the CWD has changed, the session_id is stale and
    ignored — no separate "clear on CWD change" step needed.

    Race guard: if the incoming session_id matches what's already cached
    but the cached CWD doesn't match the current CWD, this is a stale
    write from a pre-CWD-change hook — reject it. This prevents the Stop
    hook from re-polluting a session_id that was invalidated by a CWD
    change.
    """
    if not sid:
        return
    try:
        session_dir = ensure_session_dir(name)
        id_file = session_dir / "claude_session_id"
        cwd = get_claude_session_cwd(name) or ""
        old_content = id_file.read_text().strip() if id_file.exists() else ""
        old_lines = old_content.split("\n", 1) if old_content else []
        old_sid = old_lines[0].strip() if old_lines else ""
        old_cwd = old_lines[1].strip() if len(old_lines) > 1 else ""

        # Race guard: same session_id being rewritten after CWD changed.
        # The hook is still sending the old session_id from the previous
        # directory — don't let it rebind to the new CWD.
        if (sid == old_sid and old_cwd and cwd and
                old_cwd.rstrip("/") != cwd.rstrip("/")):
            _log(_LOG_WARN, "session",
                 f"{name}: rejecting stale session_id write "
                 f"(sid={sid[:12]}, old_cwd={old_cwd}, current_cwd={cwd})")
            return

        if old_sid != sid:
            _log_session_event(name, sid, cwd, "cache")
        _tmp = id_file.with_suffix('.tmp')
        _tmp.write_text(f"{sid}\n{cwd}")
        _tmp.chmod(0o600)
        os.replace(str(_tmp), str(id_file))
    except OSError as exc:
        _log(_LOG_DEBUG, "io:_cache_session_id", f"{type(exc).__name__}: {exc}")



def get_claude_session_id(name: str, authoritative: bool = False) -> str:
    """Return the Claude Code session UUID for a worker.

    The local `claude_session_id` file stores two lines:
        line 1: session UUID
        line 2: CWD path the session was created in

    Self-validating: if the stored CWD doesn't match the worker's current
    CWD, the session_id is stale (from a previous directory) and ignored.
    This prevents --resume with a wrong session after CWD changes, even if
    the Stop hook wrote back the old session_id in a race window.

    Old files with only a UUID (no line 2) are treated as valid for
    backwards compatibility.

    The per-worker cache is ALWAYS preferred over _scan_latest_session_id()
    because the scan picks the newest JSONL by mtime in the project dir,
    which is ambiguous when multiple workers share the same CWD.
    """
    cache_file = get_session_dir(name) / "claude_session_id"
    current_cwd = get_claude_session_cwd(name)

    def _read_cache() -> str:
        """Read cached session ID, validating CWD if present."""
        if not cache_file.exists():
            return ""
        content = cache_file.read_text().strip()
        if not content:
            return ""
        lines = content.split("\n", 1)
        sid = lines[0].strip()
        if not sid:
            return ""
        # Validate CWD binding (line 2) against current CWD
        if len(lines) > 1:
            cached_cwd = lines[1].strip()
            if (cached_cwd and current_cwd and
                    cached_cwd.rstrip("/") != current_cwd.rstrip("/")):
                _log(_LOG_INFO, "session",
                     f"{name}: stale session_id (cached_cwd={cached_cwd}, "
                     f"current_cwd={current_cwd})")
                return ""
        return sid

    # Both modes: prefer per-worker cache (worker-specific, set by Stop hook).
    # Only fall back to CWD-based scan when cache is empty or stale.
    val = _read_cache()
    if val:
        return val
    # Cache empty or stale — scan as fallback to self-heal
    if current_cwd:
        import bridge
        host = bridge.get_worker_host(name)
        scanned = _scan_latest_session_id(current_cwd, host=host)
        if scanned:
            _cache_session_id(name, scanned)
            return scanned
    return ""



def get_claude_session_cwd(name: str) -> str | None:
    """Get the current working directory for a worker.

    Derives from RAM hint (set by save_claude_session_cwd / checkin),
    NOT from a file. tmux pane_current_path is the upstream source of truth;
    callers that need live data should read tmux directly via _get_tmux_pane_cwd.
    """
    import bridge
    cwd = bridge._get_worker_cwd(name)
    if cwd:
        return os.path.expanduser(cwd)
    return None


def save_claude_session_cwd(name: str, cwd: str) -> None:
    """Cache a worker's CWD in RAM (no file persistence).

    The source of truth is tmux pane_current_path. This RAM hint is used
    by _get_startup_cwd for restart/teleport flows.
    """
    import bridge
    if cwd:
        cwd = os.path.expanduser(cwd)
    bridge._set_worker_cwd(name, cwd)



def clear_claude_session_id(name: str) -> None:
    """Remove the cached session ID for a worker."""
    id_file = get_session_dir(name) / "claude_session_id"
    if id_file.exists():
        id_file.unlink()



def get_any_session_id(name: str) -> tuple[str, str]:
    """Get any *_session_id value for a worker (backend-agnostic).

    Returns (session_id, source) tuple where source is the prefix (e.g. 'claude', 'codex').
    Returns ('', '') when no session ID is found.
    """
    session_dir = get_session_dir(name)
    if not session_dir.exists():
        return "", ""
    for f in sorted(session_dir.glob("*_session_id")):
        val = f.read_text().strip()
        if val:
            source = f.name.replace("_session_id", "")
            return val, source
    return "", ""



# (pending locks moved to processes.pending_locks)
# (pending locks guard moved to processes.pending_locks_guard)



# (remote home cache moved to remote_cache.home_dirs)

def _get_remote_home(host: str | None) -> str:
    """Get remote $HOME with caching (avoids SSH per message)."""
    with remote_cache.lock:
        cached = remote_cache.home_dirs.get(host or "")
    if cached is not None:
        return cached
    try:
        r = _remote_run(["bash", "-c", "echo $HOME"], host=host,
                        capture_output=True, text=True, timeout=TIMEOUT_TMUX_SEND)
        home = r.stdout.strip() if r.returncode == 0 else ""
    except (subprocess.SubprocessError, OSError):
        home = ""
    with remote_cache.lock:
        remote_cache.home_dirs[host or ""] = home
    return home



POISON_PATTERNS = [
    re.compile(r"error.*overloaded", re.IGNORECASE),
    re.compile(r"error.*401", re.IGNORECASE),
    re.compile(r"error.*403", re.IGNORECASE),
    re.compile(r"error.*429", re.IGNORECASE),
    re.compile(r"image.*dimensions.*exceed", re.IGNORECASE),
    re.compile(r"context.*(length|window).*exceed", re.IGNORECASE),
    re.compile(r"context_length_exceeded", re.IGNORECASE),
    re.compile(r"rate.?limit", re.IGNORECASE),
    re.compile(r"invalid.*api.?key", re.IGNORECASE),
    re.compile(r"invalid_request_error", re.IGNORECASE),
    re.compile(r"insufficient_quota", re.IGNORECASE),
    re.compile(r"model.*not.*found", re.IGNORECASE),
    re.compile(r"APIError", re.IGNORECASE),
    re.compile(r"connection.*reset", re.IGNORECASE),
    re.compile(r"timeout.*error", re.IGNORECASE),
    re.compile(r"error.*529", re.IGNORECASE),
    re.compile(r"error.*503", re.IGNORECASE),
]



def _capture_pane_text(tmux_name: str, lines: int = 50, host: str | None = None) -> str:
    """Return the last N lines of a tmux pane, or empty string on error."""
    if lines <= 0:
        return ""
    try:
        result = _remote_run(
            ["tmux", "capture-pane", "-t", tmux_name, "-p", "-S", f"-{lines}"],
            host=host, capture_output=True, text=True, timeout=TIMEOUT_TMUX_SEND
        )
    except (subprocess.SubprocessError, OSError):
        return ""
    if result.returncode != 0:
        return ""
    return result.stdout



HOOK_FAILURE_THRESHOLD = 3   # failures in window → POISONED

HOOK_FAILURE_WINDOW = 120    # seconds



# ─────────────────────────────────────────────────────────────────────────────
# Worker Backend Helpers
# ─────────────────────────────────────────────────────────────────────────────

def normalize_backend(backend: str | None) -> str:
    """Return a normalized backend name with a safe default."""
    return backend or DEFAULT_BACKEND



def normalize_cwd(cwd: str | None) -> str:
    """Expand ~ and return absolute path; empty string for unset/blank."""
    if cwd is None:
        return ""
    raw = cwd.strip()
    if not raw:
        return ""
    return os.path.abspath(os.path.expanduser(raw))



def validate_cwd(cwd: str | None, host: str | None = None) -> tuple[str, str]:
    """Validate cwd path. Returns (normalized_path, error_message).

    When host is set, validates via SSH on the remote machine instead of locally.
    """
    normalized = normalize_cwd(cwd)
    if not normalized:
        return "", "cwd is empty"
    if host:
        # Remote validation: check directory exists on the remote host
        try:
            r = _remote_run(["test", "-d", normalized], host=host,
                            capture_output=True, timeout=TIMEOUT_REMOTE_CMD)
            if r.returncode != 0:
                return "", f"cwd does not exist on {host}: {normalized}"
        except (subprocess.SubprocessError, OSError) as e:
            return "", f"cwd check failed on {host}: {e}"
    else:
        if not os.path.exists(normalized):
            return "", f"cwd does not exist: {normalized}"
        if not os.path.isdir(normalized):
            return "", f"cwd is not a directory: {normalized}"
    return normalized, ""



# Type alias for activity-check functions used by _extract_activity cascade.
_ActivityCheck = Callable[[list[str]], str | None]



# Interactive footer patterns (kept in sync with _extract_activity step 3b)
_INTERACTIVE_FOOTERS = [
    "Enter to select",      # AskUserQuestion single-select
    "Space to toggle",      # AskUserQuestion multi-select
    "Tab to toggle",        # Toggle confirm
    "Type to search",       # Searchable list
    "Enter to submit",      # Text submission prompt
    "Enter to add",         # Autocomplete
    "Enter to retry",       # Retry prompt
    "Enter to continue",    # Continue/proceed prompt
    "Enter to try again",   # Retry variant
    "Enter to confirm",     # Selection confirm variant
    "ctrl-g to edit",       # ExitPlanMode plan approval (editor configured)
    "Auto-approving in",    # ExitPlanMode auto-approve countdown
    "Press any key to intervene",  # ExitPlanMode auto-approve variant
]


# Content patterns that indicate an interactive prompt even without a matching footer.
# These are checked BEFORE the ❯ idle-prompt detection (step 3c) to avoid misclassifying
# the ❯ selection cursor as the text input prompt.
_INTERACTIVE_CONTENT = [
    # ExitPlanMode "Ready to code?" prompt
    "Would you like to proceed?",
    "written up a plan and is ready to execute",
    # EnterPlanMode prompt
    "wants to enter plan mode",
    "No code changes will be made until you approve",
    # Tool permission prompts
    "Allow Bash",
    "Allow Read",
    "Allow Write",
    "Allow Edit",
    "Allow Glob",
    "Allow Grep",
    "Allow Agent",
    "Allow Notebook",
]



def get_worker_backend(name: str, session: RegistryWorkerDict | TmuxSessionDict | None = None) -> str:
    """Get backend for a worker.

    Source of truth: workers.json registry (via session dict).
    The per-worker backend file has been removed — backend is set on /hire
    and stored in the registry only (no duplication).
    """
    if session and session.get("backend"):
        return normalize_backend(str(session.get("backend")))
    # Fall back to registry lookup
    import bridge
    registry = bridge._load_registry()
    entry = registry.get("workers", {}).get(name, {})
    if entry.get("backend"):
        return normalize_backend(str(entry["backend"]))
    return DEFAULT_BACKEND



# ─────────────────────────────────────────────────────────────────────────────
# CORE: WorkerManager
# ─────────────────────────────────────────────────────────────────────────────



# ─────────────────────────────────────────────────────────────────────────────
# grug say: one place for backend branching. no scatter.
# Worker Helpers (centralize backend switching)
# ─────────────────────────────────────────────────────────────────────────────



def get_tmux_env_value(tmux_name: str, key: str) -> str:
    """Get a tmux session environment variable value."""
    result = _subprocess_runner.run(
        ["tmux", "show-environment", "-t", tmux_name, key],
        capture_output=True, text=True, timeout=TIMEOUT_TMUX_CHECK
    )
    if result.returncode != 0:
        return ""
    value = result.stdout.strip()
    if "=" not in value:
        return ""
    return value.split("=", 1)[1]



def tmux_prompt_empty(tmux_name: str, timeout: float=0.5, host: str | None = None) -> bool:
    """Check if Claude Code's input prompt is empty (message was accepted).

    After sending a message, polls the tmux pane to verify the prompt
    line (❯) is empty, indicating Claude accepted the input.

    Returns True if prompt is empty within timeout, False otherwise.
    """
    import re
    start = _clock.time()
    while _clock.time() - start < timeout:
        result = _remote_run(
            ["tmux", "capture-pane", "-t", tmux_name, "-p"],
            host=host, capture_output=True, text=True, timeout=TIMEOUT_TMUX_CHECK
        )
        if result.returncode == 0:
            # Check for empty prompt: line starting with ❯ followed by only whitespace
            if re.search(r'^❯\s*$', result.stdout, re.MULTILINE):
                return True
        _clock.sleep(DELAY_BRIEF * 2)
    return False



def export_hook_env(tmux_name: str, backend: str = DEFAULT_WORKER_BACKEND, host: str | None = None) -> None:
    """Export env vars for hook inside tmux session.

    Uses tmux set-environment which persists in session and survives restarts.
    Hook reads these via `tmux show-environment -t $SESSION_NAME`.

    For remote hosts, remaps SESSIONS_DIR to use the remote $HOME prefix
    (e.g., /home/claude/... → /Users/beastoinagents/...).
    """
    # Guard: don't overwrite env if session belongs to another live bridge.
    # Prevents test/dev bridges from clobbering prod workers.
    our_url = (BRIDGE_PUBLIC_URL or BRIDGE_URL) if host else BRIDGE_URL
    try:
        r = _remote_run(["tmux", "show-environment", "-t", tmux_name, "BRIDGE_URL"],
                        host=host, capture_output=True, text=True, timeout=TIMEOUT_TMUX_CHECK)
        existing = r.stdout.strip().split("=", 1)[-1] if r.returncode == 0 else ""
        if existing and existing != our_url:
            import urllib.request
            _urlopen(existing, timeout=TIMEOUT_THREAD_JOIN).read()
            _log(_LOG_DEBUG, "worker", f"  SKIP export_hook_env({tmux_name}): owned by live bridge at {existing}")
            return
    except (urllib.error.URLError, OSError, TimeoutError):
        pass  # intentional no-op: other bridge dead or unreachable — safe to claim port

    _remote_run(["tmux", "set-environment", "-t", tmux_name, "PORT", str(PORT)], host=host, timeout=TIMEOUT_TMUX_CHECK)
    _remote_run(["tmux", "set-environment", "-t", tmux_name, "TMUX_PREFIX", TMUX_PREFIX], host=host, timeout=TIMEOUT_TMUX_CHECK)
    # Remap SESSIONS_DIR for remote hosts (different $HOME path)
    sessions_dir_val = str(SESSIONS_DIR)
    if host:
        try:
            r = _remote_run(["bash", "-c", "echo $HOME"], host=host,
                            capture_output=True, text=True, timeout=TIMEOUT_TMUX_SEND)
            remote_home = r.stdout.strip() if r.returncode == 0 else ""
            local_home = str(Path.home())
            if remote_home and remote_home != local_home and sessions_dir_val.startswith(local_home):
                sessions_dir_val = remote_home + sessions_dir_val[len(local_home):]
        except (subprocess.SubprocessError, OSError) as exc:
            _log(_LOG_DEBUG, "probe:unknown", f"{type(exc).__name__}: {exc}")
    _remote_run(["tmux", "set-environment", "-t", tmux_name, "SESSIONS_DIR", sessions_dir_val], host=host, timeout=TIMEOUT_TMUX_CHECK)
    _remote_run(["tmux", "set-environment", "-t", tmux_name, "WORKER_BACKEND", normalize_backend(backend)], host=host, timeout=TIMEOUT_TMUX_CHECK)
    # Always export BRIDGE_URL so workers know where their bridge is
    # Remote workers need BRIDGE_PUBLIC_URL (reachable IP), not localhost
    bridge_url_val = (BRIDGE_PUBLIC_URL or BRIDGE_URL) if host else BRIDGE_URL
    _remote_run(["tmux", "set-environment", "-t", tmux_name, "BRIDGE_URL", bridge_url_val], host=host, timeout=TIMEOUT_TMUX_CHECK)


