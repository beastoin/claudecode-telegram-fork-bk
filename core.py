"""Shared infrastructure for the claudecode-telegram bridge.

This module contains types, constants, DI seams, and logging used by
telegram.py, claudecode.py, and bridge.py. It imports nothing from
those modules — dependencies flow one direction only:

    core.py  (this file — foundation, no internal imports)
      ↓
    telegram.py      (Telegram API, owns ChatId/MessageId/etc.)
    claudecode.py    (worker management, owns WorkerState/TmuxSession/etc.)
      ↓
    bridge.py        (composition root — HTTP server, routing, wires everything)

Each downstream module can be imported independently:
    python3 -c "import core"         # works
    python3 -c "import telegram"     # works (imports core, not bridge)
    python3 -c "import claudecode"   # works (imports core, not bridge)
"""

from __future__ import annotations

import http.client
import os
import subprocess
import sys
import time
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Literal, Protocol, runtime_checkable


# ── Version ────────────────────────────────────────────────────────────

VERSION = "0.47.0"


# ── Safe JSON field accessors ──────────────────────────────────────────
# TypedDict .get() returns field type | None for total=False.
# These narrow to concrete types so callers can .strip(), compare, etc.

def _str_field(d: Mapping[str, object], key: str, default: str = "") -> str:
    """Extract a string field from a parsed JSON dict, with type narrowing."""
    val = d.get(key, default)
    return str(val) if val is not None else default


def _int_field(d: Mapping[str, object], key: str, default: int = 0) -> int:
    """Extract an int field from a parsed JSON dict, with type narrowing."""
    val = d.get(key, default)
    if isinstance(val, int):
        return val
    if isinstance(val, str):
        try:
            return int(val)
        except ValueError:
            return default
    return default


def _dict_field(d: Mapping[str, object], key: str) -> Mapping[str, object]:
    """Extract a dict field, returning empty dict if missing or wrong type."""
    val = d.get(key)
    return val if isinstance(val, dict) else {}


def _bool_field(d: Mapping[str, object], key: str, default: bool = False) -> bool:
    """Extract a bool field from a parsed JSON dict, with type narrowing."""
    val = d.get(key, default)
    return bool(val)


# ── Structured logging ─────────────────────────────────────────────────

_LOG_ERROR: str = "ERROR"
_LOG_WARN: str = "WARN"
_LOG_INFO: str = "INFO"
_LOG_DEBUG: str = "DEBUG"


def _log(level: str, component: str, msg: str | Path, *,
         exc: BaseException | None = None) -> None:
    """Emit a structured log line to stderr.

    Format: [LEVEL:component] message
    Optionally appends a traceback if exc is provided.
    """
    print(f"[{level}:{component}] {msg}", file=sys.stderr, flush=True)
    if exc is not None:
        import traceback as _tb
        _tb.print_exception(type(exc), exc, exc.__traceback__, file=sys.stderr)


def _log_best_effort(label: str, func: Callable[..., object], *args: object, **kwargs: object) -> object | None:  # type: ignore[explicit-any]
    """Call func(*args, **kwargs) and log on failure instead of crashing.

    Returns the function result on success, None on failure.
    """
    try:
        return func(*args, **kwargs)
    except Exception as exc:
        _log(_LOG_DEBUG, label, f"{type(exc).__name__}: {exc}")
        return None


# ── DI seams (injectable for testing) ──────────────────────────────────

class MarkdownToken(Protocol):
    """Protocol for markdown-it-py inline tokens."""
    type: str
    content: str
    children: list['MarkdownToken'] | None
    attrs: dict[str, str] | None


class SubprocessRunner(Protocol):
    """Abstraction over subprocess.run and subprocess.Popen for test injection."""

    def run(self, args: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        """Execute a subprocess command and wait for completion."""
        ...

    def popen(self, args: list[str], **kwargs: object) -> subprocess.Popen[str]:
        """Spawn a subprocess without waiting for completion."""
        ...


class Clock(Protocol):
    """Abstraction over time for deterministic testing."""

    def time(self) -> float:
        """Return the current time in seconds since epoch."""
        ...

    def sleep(self, seconds: float) -> None:
        """Sleep for the given number of seconds."""
        ...


class _RealSubprocessRunner:
    """Production subprocess runner — delegates to subprocess.run/Popen."""

    def run(self, args: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:  # type: ignore[no-any-return]
        return subprocess.run(args, **kwargs)  # type: ignore[call-overload,no-any-return]

    def popen(self, args: list[str], **kwargs: object) -> subprocess.Popen[str]:  # type: ignore[no-any-return]
        return subprocess.Popen(args, **kwargs)  # type: ignore[call-overload,no-any-return]


class _RealClock:
    """Production clock — delegates to the time module."""

    def time(self) -> float:
        return time.time()

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)


# Module-level singletons (overridable in tests)
_subprocess_runner: SubprocessRunner = _RealSubprocessRunner()  # type: ignore[explicit-any]
_clock: Clock = _RealClock()
_urlopen: Callable[..., http.client.HTTPResponse] = urllib.request.urlopen  # type: ignore[explicit-any]


# ── Node-derived configuration ─────────────────────────────────────────
# NODE_NAME drives defaults for PORT, TMUX_PREFIX, SESSIONS_DIR.
# Explicit env vars always override.

NODE_NAME = os.environ.get("NODE_NAME", "")

_DEFAULT_PORTS = {"prod": 8271, "dev": 8272, "test": 8295}

if NODE_NAME and not os.environ.get("PORT"):
    PORT = _DEFAULT_PORTS.get(NODE_NAME, 8270)
else:
    PORT = int(os.environ.get("PORT", "8270"))

BRIDGE_BIND = os.environ.get("BRIDGE_BIND", "127.0.0.1")

if NODE_NAME and not os.environ.get("SESSIONS_DIR"):
    SESSIONS_DIR = Path.home() / ".claude" / "telegram" / "nodes" / NODE_NAME / "sessions"
else:
    SESSIONS_DIR = Path(os.environ.get("SESSIONS_DIR", Path.home() / ".claude" / "telegram" / "sessions"))

if NODE_NAME and not os.environ.get("TMUX_PREFIX"):
    TMUX_PREFIX = f"claude-{NODE_NAME}-"
else:
    TMUX_PREFIX = os.environ.get("TMUX_PREFIX", "claude-")

CLAUDE_DIR = Path(os.environ.get("CLAUDE_DIR", Path.home() / ".claude"))
CLAUDE_SETTINGS_FILE = Path(os.environ.get("CLAUDE_SETTINGS_FILE", CLAUDE_DIR / "settings.json"))

# BRIDGE_URL: hook callback target. Only non-localhost URLs honored from env.
_bridge_url_env = os.environ.get("BRIDGE_URL", "").rstrip("/")
if _bridge_url_env and not _bridge_url_env.startswith(("http://localhost", "http://127.0.0.1")):
    BRIDGE_URL = _bridge_url_env
else:
    BRIDGE_URL = f"http://localhost:{PORT}"

BRIDGE_PUBLIC_URL = os.environ.get("BRIDGE_PUBLIC_URL", "").rstrip("/")
if BRIDGE_PUBLIC_URL and not os.environ.get("BRIDGE_BIND"):
    BRIDGE_BIND = "0.0.0.0"

BRIDGE_SSH_TARGET = os.environ.get("BRIDGE_SSH_TARGET", "vps")

WEBHOOK_SECRET = os.environ.get("TELEGRAM_WEBHOOK_SECRET", "")

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")

NODE_DIR = SESSIONS_DIR.parent if NODE_NAME else SESSIONS_DIR.parent

# Derived node name for /tmp isolation
_node_name = TMUX_PREFIX.strip("-").removeprefix("claude-") or "default"

MACHINES_CONFIG_FILE = Path(os.environ.get(
    "MACHINES_CONFIG_FILE",
    Path.home() / ".config" / "claudecode-telegram" / "machines.json"
))


# ── Timeouts (seconds) ─────────────────────────────────────────────────

# Subprocess
TIMEOUT_TMUX_CHECK = 3
TIMEOUT_TMUX_SEND = 5
TIMEOUT_REMOTE_CMD = 10
TIMEOUT_FILE_TRANSFER = 15
TIMEOUT_GIT_OP = 30
TIMEOUT_LARGE_TRANSFER = 60
TIMEOUT_RSYNC = 120
TIMEOUT_FULL_SYNC = 600

# HTTP
TIMEOUT_HTTP_API = 10
TIMEOUT_HTTP_DOWNLOAD = 30
TIMEOUT_HTTP_UPLOAD = 60

# Process lifecycle
TIMEOUT_PROCESS_WAIT = 3
TIMEOUT_THREAD_JOIN = 1.0

# Delays
DELAY_TMUX_SEND = 0.3
DELAY_PIPE_POLL = 0.5
DELAY_STARTUP = 1.0
DELAY_STARTUP_LONG = 1.5
DELAY_RETRY = 0.5
DELAY_BRIEF = 0.05
DELAY_SHORT = 0.2
DELAY_RESPONSE_GAP = 2
DELAY_PROCESS_SETTLE = 3
DELAY_CLAUDE_LOAD = 4


# ── Worker/session defaults ────────────────────────────────────────────

DEFAULT_BACKEND = "claude"
DEFAULT_WORKER_BACKEND = DEFAULT_BACKEND
PENDING_TIMEOUT = 600

FILE_INBOX_ROOT = Path(f"/tmp/claudecode-telegram/{_node_name}")
WORKER_PIPE_ROOT = Path(f"/tmp/claudecode-telegram/{_node_name}")

SANDBOX_ENABLED = os.environ.get("SANDBOX_ENABLED", "0") == "1"
SANDBOX_IMAGE = os.environ.get("SANDBOX_IMAGE", "claudecode-telegram:latest")
SANDBOX_EXTRA_MOUNTS: list[Any] = []
_mounts_env = os.environ.get("SANDBOX_MOUNTS", "")

TEAM_DIR = os.path.expanduser(os.environ.get("TEAM_DIR", "~/team"))
_CHECKIN_NOTE_PATH = os.path.join(TEAM_DIR, "checkin-note.txt")
_LEARNING_REMINDER_PATH = os.path.join(TEAM_DIR, "learning-reminder.txt")

PERSISTENCE_NOTE = "They'll stay on your team."

# Voice mode: STT
STT_ENDPOINT = os.environ.get("STT_ENDPOINT", "http://100.126.187.125:10110/transcribe")
STT_TIMEOUT = int(os.environ.get("STT_TIMEOUT", "10"))

ADMIN_CHAT_ID_ENV = os.environ.get("ADMIN_CHAT_ID", "")
admin_chat_id: int | None = int(ADMIN_CHAT_ID_ENV) if ADMIN_CHAT_ID_ENV else None


# ── Config dataclasses ──────────────────────────────────────────────────

@dataclass(frozen=True)
class WatchdogConfig:
    """Watchdog timing and threshold configuration (immutable)."""
    interval: int = 4
    start_grace: int = 30
    think_grace: int = 30
    tool_gap_grace: int = 12
    stale_pending: int = 900
    cpu_active: float = 15.0
    cpu_idle: float = 7.0
    idle_streak_stuck: int = 3
    alert_cooldown: int = 180
    restart_cooldown: int = 60
    host_down_threshold: int = 3


@dataclass(frozen=True)
class ResourceAlertConfig:
    """Resource monitoring thresholds and cooldowns (immutable)."""
    disk_warn_pct: int = 85
    disk_alert_pct: int = 95
    disk_alert_gb: int = 5
    disk_cooldown: int = 3600
    cpu_hog_pct: int = 90
    cpu_hog_duration_min: int = 60
    cpu_hog_cooldown: int = 3600
    worktree_threshold_gb: int = 30
    worktree_cooldown: int = 3600
    mem_threshold_pct: int = 90
    mem_threshold_gb: int = 4
    mem_cooldown: int = 3600
    io_iowait_pct: int = 30
    io_cooldown: int = 3600
    infra_cooldown: int = 300


@dataclass(frozen=True)
class MediaConfig:
    """Media handling limits and extension sets (immutable)."""
    max_file_size: int = 50 * 1024 * 1024
    photo_max_sum: int = 10000
    photo_max_dim: int = 5000


_wd_cfg = WatchdogConfig()
_res_cfg = ResourceAlertConfig()

WATCHDOG_INTERVAL = _wd_cfg.interval
START_GRACE = _wd_cfg.start_grace
THINK_GRACE = _wd_cfg.think_grace
TOOL_GAP_GRACE = _wd_cfg.tool_gap_grace
STALE_PENDING = _wd_cfg.stale_pending
CPU_ACTIVE = _wd_cfg.cpu_active
CPU_IDLE = _wd_cfg.cpu_idle
IDLE_STREAK_STUCK = _wd_cfg.idle_streak_stuck
ALERT_COOLDOWN = _wd_cfg.alert_cooldown
RESTART_COOLDOWN = _res_cfg.disk_cooldown  # re-exported for compat


# ── Derived resource constants ──────────────────────────────────────────

DISK_WARN_PCT = _res_cfg.disk_warn_pct
DISK_ALERT_PCT = _res_cfg.disk_alert_pct
DISK_ALERT_GB = _res_cfg.disk_alert_gb
DISK_COOLDOWN = _res_cfg.disk_cooldown
CPU_HOG_PCT = _res_cfg.cpu_hog_pct
CPU_HOG_DURATION_MIN = _res_cfg.cpu_hog_duration_min
CPU_HOG_COOLDOWN = _res_cfg.cpu_hog_cooldown
WORKTREE_THRESHOLD_GB = _res_cfg.worktree_threshold_gb
WORKTREE_COOLDOWN = _res_cfg.worktree_cooldown
MEM_THRESHOLD_PCT = _res_cfg.mem_threshold_pct
MEM_THRESHOLD_GB = _res_cfg.mem_threshold_gb
MEM_COOLDOWN = _res_cfg.mem_cooldown
IO_IOWAIT_PCT = _res_cfg.io_iowait_pct
IO_COOLDOWN = _res_cfg.io_cooldown
INFRA_COOLDOWN = _res_cfg.infra_cooldown

HOST_DOWN_THRESHOLD = _wd_cfg.host_down_threshold


# ── AppContext: injectable configuration ────────────────────────────────

TRANSPORT_MODE = os.environ.get("TRANSPORT_MODE", "telegram")

@dataclass
class AppContext:
    """Injectable application configuration — replaces scattered module globals."""
    bot_token: str = ""
    port: int = 8270
    bridge_bind: str = "127.0.0.1"
    bridge_url: str = ""
    bridge_public_url: str = ""
    bridge_ssh_target: str = "vps"
    sessions_dir: Path | None = None
    tmux_prefix: str = "claude-"
    node_name: str = ""
    claude_dir: Path | None = None
    default_backend: str = "claude"
    sandbox_enabled: bool = False
    sandbox_image: str = ""
    team_dir: str = ""
    watchdog_interval: int = 4
    webhook_secret: str = ""
    transport_mode: str = "telegram"

    def __post_init__(self) -> None:
        if self.sessions_dir is None:
            self.sessions_dir = Path.home() / ".claude" / "telegram" / "sessions"
        if self.claude_dir is None:
            self.claude_dir = Path.home() / ".claude"
        if not self.bridge_url:
            self.bridge_url = f"http://localhost:{self.port}"


def _build_app_context() -> AppContext:
    """Build AppContext from current module globals."""
    return AppContext(
        bot_token=BOT_TOKEN,
        port=PORT,
        bridge_bind=BRIDGE_BIND,
        bridge_url=BRIDGE_URL,
        bridge_public_url=BRIDGE_PUBLIC_URL,
        bridge_ssh_target=BRIDGE_SSH_TARGET,
        sessions_dir=SESSIONS_DIR,
        tmux_prefix=TMUX_PREFIX,
        node_name=NODE_NAME,
        claude_dir=CLAUDE_DIR,
        default_backend=DEFAULT_BACKEND,
        sandbox_enabled=SANDBOX_ENABLED,
        sandbox_image=SANDBOX_IMAGE,
        team_dir=TEAM_DIR,
        watchdog_interval=WATCHDOG_INTERVAL,
        webhook_secret=WEBHOOK_SECRET,
        transport_mode=TRANSPORT_MODE,
    )


_app_context: AppContext | None = None

def get_app_context() -> AppContext:
    """Get the singleton AppContext. Built on first call from module globals."""
    global _app_context
    if _app_context is None:
        _app_context = _build_app_context()
    return _app_context
