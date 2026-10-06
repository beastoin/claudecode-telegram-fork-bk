"""Tests for the claudecode module — worker management, backends, sessions.

Behavior tests: each test verifies something the bridge or manager
actually depends on, not that code structure exists.
"""
import claudecode
import bridge

import json
import os
from pathlib import Path
from unittest.mock import MagicMock


# ── Watchdog state machine: worker health classification ─────────────


def test_idle_worker_is_ready():
    """Worker with tmux alive, claude running, nothing pending → READY."""
    state, _ = bridge.compute_state(
        tmux_exists=True, claude_pid="1234", pending=False,
        pending_ts=None, pending_age=0, children=0,
        last_child_ts=0, cpu=0.0, last_hook_ts=None,
        last_seen_claude=None, now=1000, is_interactive=True,
        adapter_alive=False, poisoned_reason=None,
    )
    assert state == "READY"


def test_worker_running_tools_is_busy():
    """Worker with pending + children → BUSY_TOOL (running subprocesses)."""
    state, _ = bridge.compute_state(
        tmux_exists=True, claude_pid="1234", pending=True,
        pending_ts=900, pending_age=100, children=3,
        last_child_ts=950, cpu=5.0, last_hook_ts=None,
        last_seen_claude=None, now=1000, is_interactive=True,
        adapter_alive=False, poisoned_reason=None,
    )
    assert state == "BUSY_TOOL"


def test_tmux_gone_is_offline():
    """When tmux session is gone, worker is OFFLINE."""
    state, _ = bridge.compute_state(
        tmux_exists=False, claude_pid=None, pending=False,
        pending_ts=None, pending_age=0, children=0,
        last_child_ts=0, cpu=0.0, last_hook_ts=None,
        last_seen_claude=None, now=1000, is_interactive=True,
        adapter_alive=False, poisoned_reason=None,
    )
    assert state == "OFFLINE"


def test_error_loop_detected_as_poisoned():
    """Long-stale pending + poisoned_reason → POISONED (needs --clean restart)."""
    state, _ = bridge.compute_state(
        tmux_exists=True, claude_pid="1234", pending=True,
        pending_ts=50, pending_age=950, children=0,
        last_child_ts=50, cpu=0.0, last_hook_ts=None,
        last_seen_claude=None, now=1000, is_interactive=True,
        adapter_alive=False, poisoned_reason="3 consecutive tool errors",
    )
    assert state == "POISONED"


def test_stale_pending_without_poison_is_stuck():
    """Long-stale pending without poison reason → STUCK."""
    state, _ = bridge.compute_state(
        tmux_exists=True, claude_pid="1234", pending=True,
        pending_ts=50, pending_age=950, children=0,
        last_child_ts=50, cpu=0.0, last_hook_ts=None,
        last_seen_claude=None, now=1000, is_interactive=True,
        adapter_alive=False, poisoned_reason=None,
    )
    assert state == "STUCK"


def test_noninteractive_adapter_running_is_busy():
    """Codex backend with adapter alive → BUSY_TOOL."""
    state, _ = bridge.compute_state(
        tmux_exists=True, claude_pid=None, pending=False,
        pending_ts=None, pending_age=0, children=0,
        last_child_ts=0, cpu=0.0, last_hook_ts=None,
        last_seen_claude=None, now=1000, is_interactive=False,
        adapter_alive=True, poisoned_reason=None,
    )
    assert state == "BUSY_TOOL"


# ── Backend properties: hire command uses these ──────────────────────


def test_claude_is_interactive():
    """ClaudeBackend is interactive — sends via tmux send-keys."""
    backend = claudecode.ClaudeBackend()
    assert backend.name == "claude"
    assert backend.is_interactive is True


def test_codex_is_exec():
    """CodexBackend is exec mode — no tmux session, API calls only."""
    backend = claudecode.CodexBackend()
    assert backend.name == "codex"
    assert backend.is_interactive is False
    assert backend.is_exec is True


def test_normalize_backend_defaults_to_claude():
    """None/empty backend normalizes to 'claude' (backward compat)."""
    assert claudecode.normalize_backend(None) == "claude"
    assert claudecode.normalize_backend("") == "claude"
    assert claudecode.normalize_backend("codex") == "codex"


# ── Session directory lifecycle: created on hire, cleaned on end ─────


def test_session_dir_created_on_ensure(tmp_path):
    """ensure_session_dir creates the directory (happens during /hire)."""
    bridge.SESSIONS_DIR = tmp_path
    result = claudecode.ensure_session_dir("alice")
    assert Path(result).exists()
    assert Path(result).is_dir()


def test_clear_pending_removes_file(tmp_path):
    """clear_pending removes pending message (after worker responds)."""
    bridge.SESSIONS_DIR = tmp_path
    session_dir = tmp_path / "alice"
    session_dir.mkdir()
    pending = session_dir / "pending"
    pending.write_text("what's the status?")
    bridge.clear_pending("alice")
    assert not pending.exists()


def test_clear_pending_noop_when_missing(tmp_path):
    """clear_pending doesn't crash when no pending file exists."""
    bridge.SESSIONS_DIR = tmp_path
    (tmp_path / "alice").mkdir()
    bridge.clear_pending("alice")  # should not raise


# ── Inbox lifecycle: temp files for media downloads ──────────────────


def test_inbox_created_on_ensure(tmp_path):
    """ensure_inbox_dir creates inbox/ for file downloads."""
    bridge.FILE_INBOX_ROOT = tmp_path / "inboxes"
    result = bridge.ensure_inbox_dir("alice")
    assert Path(result).exists()


def test_inbox_files_cleaned_up(tmp_path):
    """cleanup_inbox removes downloaded files after worker processes them."""
    inbox = tmp_path / "inboxes" / "alice" / "inbox"
    inbox.mkdir(parents=True)
    (inbox / "photo_123.jpg").write_bytes(b"\xff\xd8")
    (inbox / "doc_456.pdf").write_bytes(b"%PDF")
    bridge.FILE_INBOX_ROOT = tmp_path / "inboxes"
    bridge.cleanup_inbox("alice")
    assert list(inbox.iterdir()) == []


# ── Worker pipe lifecycle ────────────────────────────────────────────


def test_pipe_dir_cleaned_up(tmp_path):
    """cleanup_worker_pipe removes the pipe directory."""
    pipe_dir = tmp_path / "alice"
    pipe_dir.mkdir()
    (pipe_dir / "in.pipe").write_text("")
    bridge.WORKER_PIPE_ROOT = tmp_path
    claudecode.cleanup_worker_pipe("alice")
    assert not pipe_dir.exists()


# ── Registry: workers.json persistence ───────────────────────────────


def test_missing_registry_returns_empty(tmp_path):
    """Missing workers.json returns empty dict (first run)."""
    bridge.WORKER_REGISTRY_FILE = tmp_path / "workers.json"
    result = bridge._load_registry()
    assert result == {}


def test_corrupt_registry_returns_empty(tmp_path):
    """Corrupt registry is handled gracefully (renamed, empty returned)."""
    reg_file = tmp_path / "workers.json"
    reg_file.write_text("not json {{{")
    bridge.WORKER_REGISTRY_FILE = reg_file
    result = bridge._load_registry()
    assert result == {}


def test_valid_registry_loads(tmp_path):
    """Valid registry loads worker entries."""
    reg_file = tmp_path / "workers.json"
    reg_file.write_text(json.dumps({
        "workers": {
            "alice": {"backend": "claude", "tmux": "claude-test-alice"},
            "bob": {"backend": "codex", "tmux": "claude-test-bob"},
        }
    }))
    bridge.WORKER_REGISTRY_FILE = reg_file
    result = bridge._load_registry()
    assert "alice" in result["workers"]
    assert "bob" in result["workers"]
    assert result["workers"]["alice"]["backend"] == "claude"


# ── Session ID management ───────────────────────────────────────────


def test_session_id_empty_when_no_file(tmp_path):
    """No session_id file → empty string (first run or after --clean)."""
    bridge.SESSIONS_DIR = tmp_path
    assert claudecode.get_claude_session_id("alice") == ""


def test_clear_session_id(tmp_path):
    """clear_claude_session_id removes the cached ID (for --clean restart)."""
    bridge.SESSIONS_DIR = tmp_path
    session_dir = tmp_path / "alice"
    session_dir.mkdir()
    (session_dir / "claude_session_id").write_text("sess-abc123")
    claudecode.clear_claude_session_id("alice")
    assert not (session_dir / "claude_session_id").exists()


# ── Start command: --resume flag ─────────────────────────────────────


def test_fresh_start_has_no_resume():
    """Fresh start (no resume_id) doesn't include --resume flag."""
    cmd = claudecode.build_claude_start_cmd(resume_id=None)
    assert "--resume" not in cmd


def test_resume_includes_session_id():
    """Resume start includes --resume with the session ID."""
    cmd = claudecode.build_claude_start_cmd(resume_id="sess-abc123")
    assert "--resume" in cmd
    assert "sess-abc123" in cmd


# ── Worker host lookup ───────────────────────────────────────────────


def test_unknown_worker_host_is_none(tmp_path):
    """get_worker_host returns None for unregistered worker."""
    bridge.WORKER_REGISTRY_FILE = tmp_path / "workers.json"
    (tmp_path / "workers.json").write_text('{"workers":{}}')
    assert bridge.get_worker_host("nonexistent") is None


# ── MentionTracker: snapshot/restore for @ routing ───────────────────


def test_mention_snapshot_restore():
    """MentionTracker snapshot/restore preserves routing state across operations."""
    tracker = bridge.MentionTracker()
    tracker.target = "alice"
    tracker.ts = 100.0
    tracker.count = 3
    snap = tracker.snapshot()

    tracker.target = "bob"
    tracker.count = 5
    tracker.restore(snap)

    assert tracker.target == "alice"
    assert tracker.count == 3


# ── Split architecture invariants ────────────────────────────────────


def test_bridge_mock_reaches_claudecode():
    """Setting bridge.admin_chat_id propagates to claudecode.

    Tests that patch bridge.X must affect claudecode functions that
    read X — this is the split's core contract.
    """
    original = claudecode.admin_chat_id
    try:
        bridge.admin_chat_id = 77777
        assert claudecode.admin_chat_id == 77777
    finally:
        bridge.admin_chat_id = original


def test_proxy_propagates_to_both_modules():
    """One bridge.X write updates both telegram and claudecode."""
    import telegram
    orig = bridge.admin_chat_id
    try:
        bridge.admin_chat_id = 55555
        assert telegram.admin_chat_id == 55555
        assert claudecode.admin_chat_id == 55555
    finally:
        bridge.admin_chat_id = orig


def test_bridge_functions_see_mocked_globals():
    """Functions defined in bridge.py see mocks through __globals__.

    This is the class-swap proxy's key invariant: func.__globals__
    IS bridge.__dict__, so setattr on bridge updates what functions
    resolve by name.
    """
    handler = bridge.Handler.__dict__["handle_checkin_endpoint"]
    assert handler.__globals__ is bridge.__dict__
