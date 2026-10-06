"""Tests migrated from test.sh — backend category."""
import pytest


@pytest.fixture(autouse=True)
def _bridge_env(monkeypatch, tmp_path):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "fake:token")
    monkeypatch.setenv("ADMIN_CHAT_ID", "")
    monkeypatch.setenv("NODE_NAME", "test")
    sessions = tmp_path / "sessions"
    sessions.mkdir()
    monkeypatch.setenv("BRIDGE_SESSIONS_DIR", str(sessions))
    monkeypatch.setenv("TEAM_DIR", str(tmp_path / "team"))
    # Save/restore mutable bridge globals so tests don't leak state
    import bridge
    saved = {
        "SESSIONS_DIR": bridge.SESSIONS_DIR,
        "WORKER_PIPE_ROOT": getattr(bridge, "WORKER_PIPE_ROOT", None),
        "NODE_DIR": getattr(bridge, "NODE_DIR", None),
        "WORKER_REGISTRY_FILE": getattr(bridge, "WORKER_REGISTRY_FILE", None),
        "telegram_api": bridge.telegram_api,
        "get_registered_sessions": bridge.get_registered_sessions,
        "tmux_exists": getattr(bridge, "tmux_exists", None),
        "ClaudeBackend_send": bridge.ClaudeBackend.send,
        "CodexBackend_send": bridge.CodexBackend.send,
        "wm_scan": bridge.worker_manager.scan_tmux_sessions,
        "wm_get_reg": bridge.worker_manager.get_registered_sessions,
        "wm_is_online": bridge.worker_manager.is_online,
        "wm_send": bridge.worker_manager.send,
    }
    yield
    bridge.SESSIONS_DIR = saved["SESSIONS_DIR"]
    if saved["WORKER_PIPE_ROOT"] is not None:
        bridge.WORKER_PIPE_ROOT = saved["WORKER_PIPE_ROOT"]
    if saved["NODE_DIR"] is not None:
        bridge.NODE_DIR = saved["NODE_DIR"]
    if saved["WORKER_REGISTRY_FILE"] is not None:
        bridge.WORKER_REGISTRY_FILE = saved["WORKER_REGISTRY_FILE"]
    bridge.telegram_api = saved["telegram_api"]
    bridge.get_registered_sessions = saved["get_registered_sessions"]
    if saved["tmux_exists"] is not None:
        bridge.tmux_exists = saved["tmux_exists"]
    bridge.ClaudeBackend.send = saved["ClaudeBackend_send"]
    bridge.CodexBackend.send = saved["CodexBackend_send"]
    bridge.worker_manager.scan_tmux_sessions = saved["wm_scan"]
    bridge.worker_manager.get_registered_sessions = saved["wm_get_reg"]
    bridge.worker_manager.is_online = saved["wm_is_online"]
    bridge.worker_manager.send = saved["wm_send"]
    bridge.worker_manager.invalidate_sessions_cache()


def test_backend_env_metadata():
    import bridge
    import claudecode

    calls = []

    class FakeRunner:
        def run(self, cmd, **kwargs):
            calls.append(cmd)

            class Result:
                returncode = 0
                stdout = ""

            return Result()

        def popen(self, *a, **kw):
            pass

    saved = claudecode._subprocess_runner
    try:
        claudecode._subprocess_runner = FakeRunner()
        bridge.export_hook_env("claude-test-backend", "codex")
    finally:
        claudecode._subprocess_runner = saved

    found = any("WORKER_BACKEND" in cmd and "codex" in cmd for cmd in calls)
    assert found, f"WORKER_BACKEND=codex not set in tmux env: {calls}"


def test_hire_backend_parsing():
    from bridge import parse_hire_args, DEFAULT_WORKER_BACKEND

    name, backend = parse_hire_args("alice")
    assert name == "alice", f"expected alice, got {name}"
    assert backend == DEFAULT_WORKER_BACKEND, f"expected default backend, got {backend}"

    name, backend = parse_hire_args("--codex alice")
    assert name == "alice", f"expected alice, got {name}"
    assert backend == "codex", f"expected codex backend, got {backend}"

    name, backend = parse_hire_args("alice --codex")
    assert name == "alice", f"expected alice, got {name}"
    assert backend == "codex", f"expected codex backend, got {backend}"

    name, backend = parse_hire_args("codex-amy")
    assert name == "amy", f"expected amy, got {name}"
    assert backend == "codex", f"expected codex backend, got {backend}"


def test_team_output_includes_backend():
    from bridge import format_team_lines

    registered = {
        "alice": {"backend": "codex"},
        "bob": {"backend": "claude"},
    }

    lines = format_team_lines(registered, active="alice", pending_lookup=lambda name: False)
    text = "\n".join(lines)

    assert "backend=codex" in text, f"expected codex backend in team output: {text}"
    assert "backend=claude" in text, f"expected claude backend in team output: {text}"


def test_worker_send_uses_backend():
    import bridge

    calls = {"claude": 0, "codex": 0}

    # Mock the backend send methods
    original_claude_send = bridge.ClaudeBackend.send
    original_codex_send = bridge.CodexBackend.send

    def fake_claude_send(self, name, tmux, text, url, dir):
        calls["claude"] += 1
        return True

    def fake_codex_send(self, name, tmux, text, url, dir):
        calls["codex"] += 1
        return True

    bridge.ClaudeBackend.send = fake_claude_send
    bridge.CodexBackend.send = fake_codex_send

    try:
        # Also update the instances in BACKENDS
        bridge.BACKENDS["claude"] = bridge.ClaudeBackend()
        bridge.BACKENDS["codex"] = bridge.CodexBackend()

        def fake_scan():
            return {"alice": {"tmux": "claude-test-alice", "backend": "codex"}}

        def fake_get_registered_sessions(registered=None):
            return registered or fake_scan()

        bridge.worker_manager.scan_tmux_sessions = fake_scan
        bridge.worker_manager.get_registered_sessions = fake_get_registered_sessions

        session = fake_scan()["alice"]
        ok = bridge.worker_send("alice", "hello", session=session)

        assert ok is True, "expected worker_send to succeed"
        assert calls["codex"] == 1, f"expected codex send, got {calls}"
        assert calls["claude"] == 0, f"expected no claude send, got {calls}"
    finally:
        bridge.ClaudeBackend.send = original_claude_send
        bridge.CodexBackend.send = original_codex_send


def test_codex_end_cleans_session():
    import tempfile
    from pathlib import Path
    import bridge

    import json
    tmp = Path(tempfile.mkdtemp())
    bridge.SESSIONS_DIR = tmp
    bridge.WORKER_PIPE_ROOT = tmp / "pipes"
    bridge.WORKER_REGISTRY_FILE = tmp / "workers.json"
    # Register codex worker in registry (source of truth for backend)
    (tmp / "workers.json").write_text(json.dumps({
        "workers": {"alice": {"backend": "codex", "tmux": "claude-test-alice"}}
    }))
    bridge.worker_manager.scan_tmux_sessions = lambda: {}
    bridge._sync_worker_manager()

    session_dir = tmp / "alice"
    session_dir.mkdir()
    (session_dir / "codex_session_id").write_text("thread_123")

    # Create pipe to verify cleanup
    bridge.ensure_worker_pipe("alice")
    pipe_path = bridge.get_worker_pipe_path("alice")
    assert pipe_path.exists(), "pipe should exist before cleanup"

    ok, err = bridge.kill_session("alice")
    assert ok is True, f"expected ok, got err: {err}"
    assert not (session_dir / "codex_session_id").exists(), "session id should be removed"
    assert not pipe_path.exists(), "pipe should be removed"


def test_codex_relaunch_clears_session_id():
    import shutil
    import tempfile
    from pathlib import Path
    import bridge

    tmp = Path(tempfile.mkdtemp())
    bridge.SESSIONS_DIR = tmp
    bridge.WORKER_PIPE_ROOT = tmp / "pipes"
    bridge.WORKER_REGISTRY_FILE = tmp / "workers.json"
    # Non-interactive workers now have tmux — mock scan to return alice with tmux
    prefix = bridge.TMUX_PREFIX
    bridge.worker_manager.scan_tmux_sessions = lambda: {
        "alice": {"tmux": f"{prefix}alice", "backend": "codex"}
    }
    # Sync worker_manager so it knows about alice
    bridge._sync_worker_manager()
    # Mock tmux_exists since no real tmux session
    bridge.tmux_exists = lambda name: True
    # Mock shutil.which so binary check passes without codex installed
    orig_which = shutil.which
    shutil.which = lambda name, path=None: "/usr/bin/" + name if name == "codex" else orig_which(name, path=path)

    try:
        session_dir = tmp / "alice"
        session_dir.mkdir(exist_ok=True)
        (session_dir / "backend").write_text("codex")
        (session_dir / "codex_session_id").write_text("thread_123")

        ok, err = bridge.restart_claude("alice")
        assert ok is True, f"expected ok, got err: {err}"
        assert not (session_dir / "codex_session_id").exists(), "session id should be cleared"
        assert (session_dir / "backend").exists(), "backend file should remain"
    finally:
        shutil.which = orig_which


def test_get_workers_includes_codex():
    """Codex workers registered in workers.json appear in get_workers."""
    import tempfile, json
    from pathlib import Path
    import bridge

    tmp = Path(tempfile.mkdtemp())
    bridge.SESSIONS_DIR = tmp
    bridge.WORKER_PIPE_ROOT = tmp / "pipes"
    bridge.WORKER_REGISTRY_FILE = tmp / "workers.json"
    # Register alice as codex in the registry
    (tmp / "workers.json").write_text(json.dumps({
        "workers": {"alice": {"backend": "codex", "tmux": "claude-test-alice"}}
    }))
    bridge.worker_manager.scan_tmux_sessions = lambda: {}
    bridge._sync_worker_manager()

    bridge.ensure_worker_pipe("alice")
    workers = bridge.get_workers()
    names = [w["name"] for w in workers]
    assert "alice" in names, f"expected alice in workers, got {workers}"

    item = next(w for w in workers if w["name"] == "alice")
    assert item["protocol"] == "pipe", f"expected pipe protocol, got {item}"


def test_backend_registry_is_canonical():
    """workers.json registry is the source of truth for backend type."""
    import tempfile, json
    from pathlib import Path
    import bridge

    tmp = Path(tempfile.mkdtemp())
    bridge.SESSIONS_DIR = tmp
    bridge.WORKER_REGISTRY_FILE = tmp / "workers.json"

    # Register alice as codex in the registry
    (tmp / "workers.json").write_text(json.dumps({
        "workers": {"alice": {"backend": "codex"}}
    }))

    # get_worker_backend with a session dict that says 'claude'
    # Registry must win when session dict is absent
    result_registry = bridge.get_worker_backend("alice")
    assert result_registry == "codex", f"registry lookup: expected codex, got {result_registry}"

    # Session dict takes priority when provided (it comes from the registry anyway)
    result_with_session = bridge.get_worker_backend("alice", {"backend": "codex"})
    assert result_with_session == "codex", f"session dict: expected codex, got {result_with_session}"


def test_pipe_forwarding_to_codex():
    import tempfile
    from pathlib import Path
    import bridge

    tmp = Path(tempfile.mkdtemp())
    bridge.SESSIONS_DIR = tmp
    bridge.WORKER_PIPE_ROOT = tmp / "pipes"
    bridge.worker_manager.scan_tmux_sessions = lambda: {}

    session_dir = tmp / "alice"
    session_dir.mkdir()
    (session_dir / "backend").write_text("codex")

    called = {"codex": 0}

    def fake_send(name, text, chat_id=None, session=None):
        called["codex"] += 1
        return True

    bridge.worker_manager.send = fake_send

    bridge._forward_pipe_message("alice", "hello")
    assert called["codex"] == 1, f"expected codex send, got {called}"


def test_codex_parse_jsonl():
    import json
    import bridge

    # Simulate codex --json JSONL output
    lines = [
        json.dumps({"type": "thread.started", "thread_id": "thread_abc123"}),
        json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": "Hello world"}}),
        json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": "Second part"}}),
        json.dumps({"type": "item.completed", "item": {"type": "tool_call", "text": "ignored"}}),
    ]
    output = "\n".join(lines)

    response, thread_id = bridge._codex_parse_jsonl(output)
    assert thread_id == "thread_abc123", f"expected thread_abc123, got {thread_id!r}"
    assert "Hello world" in response, f"expected Hello world in response, got {response!r}"
    assert "Second part" in response, f"expected Second part in response, got {response!r}"
    assert "ignored" not in response, "tool_call text should not appear in response"

    # Empty output
    response2, tid2 = bridge._codex_parse_jsonl("")
    assert response2 == "", f"empty output should give empty response, got {response2!r}"
    assert tid2 == "", f"empty output should give empty thread_id, got {tid2!r}"

    # Malformed JSON lines are skipped
    response3, tid3 = bridge._codex_parse_jsonl("not json\n{bad\n")
    assert response3 == "", f"malformed should give empty response, got {response3!r}"


def test_update_bot_commands_includes_codex():
    import bridge

    captured = {}
    original_api = bridge.telegram_api
    original_get = bridge.get_registered_sessions

    def fake_api(method, payload):
        captured["payload"] = payload
        return {"ok": True}

    bridge.telegram_api = fake_api
    bridge.get_registered_sessions = lambda: {
        "alice": {"backend": "codex", "mode": "codex-exec"},
        "bob": {"backend": "claude", "tmux": "claude-test-bob"},
    }

    try:
        bridge.update_bot_commands()

        commands = [c["command"] for c in captured["payload"]["commands"]]
        assert "alice" in commands and "bob" in commands, f"expected codex worker in commands, got {commands}"
    finally:
        bridge.telegram_api = original_api
        bridge.get_registered_sessions = original_get


def test_broadcast_includes_codex():
    import bridge

    called = []
    original_get = bridge.worker_manager.get_registered_sessions
    original_online = bridge.worker_manager.is_online

    bridge.worker_manager.get_registered_sessions = lambda registered=None: {
        "alice": {"backend": "codex", "mode": "exec"},
        "bob": {"backend": "claude", "tmux": "claude-test-bob"},
    }
    bridge.worker_manager.is_online = lambda name, session=None: True

    try:
        class FakeTelegram:
            def send_message(self, *args, **kwargs):
                return {"ok": True}

        router = bridge.CommandRouter(FakeTelegram(), bridge.worker_manager)
        router.route_message = lambda name, text, chat_id, msg_id, one_off=False: called.append(name)
        router.route_to_all("hello team", 123, 456)

        assert set(called) == {"alice", "bob"}, f"expected broadcast to include codex worker, got {called}"
    finally:
        bridge.worker_manager.get_registered_sessions = original_get
        bridge.worker_manager.is_online = original_online


def test_codex_session_id_persistence():
    import tempfile
    import shutil
    from pathlib import Path
    import bridge

    tmpdir = tempfile.mkdtemp()
    tmp = Path(tmpdir)
    sessions = tmp / "sessions"
    sessions.mkdir()
    (sessions / "testworker").mkdir()

    try:
        # Initially empty
        sid = bridge._codex_load_session_id("testworker", sessions)
        assert sid == "", f"expected empty, got {sid!r}"

        # Save and reload
        bridge._codex_save_session_id("testworker", sessions, "thread_xyz789")
        sid2 = bridge._codex_load_session_id("testworker", sessions)
        assert sid2 == "thread_xyz789", f"expected thread_xyz789, got {sid2!r}"

        # Overwrite
        bridge._codex_save_session_id("testworker", sessions, "thread_new")
        sid3 = bridge._codex_load_session_id("testworker", sessions)
        assert sid3 == "thread_new", f"expected thread_new, got {sid3!r}"

        # Auto-creates directory
        bridge._codex_save_session_id("newworker", sessions, "thread_auto")
        sid4 = bridge._codex_load_session_id("newworker", sessions)
        assert sid4 == "thread_auto", f"expected thread_auto, got {sid4!r}"
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_codex_backend_send_spawns_thread():
    import tempfile
    import shutil
    import threading
    from pathlib import Path
    from unittest.mock import patch
    import bridge

    tmpdir = tempfile.mkdtemp()
    tmp = Path(tmpdir)
    orig_node = bridge.NODE_DIR
    orig_reg = bridge.WORKER_REGISTRY_FILE

    bridge.NODE_DIR = tmp
    bridge.WORKER_REGISTRY_FILE = tmp / "workers.json"

    try:
        sessions = tmp / "sessions"
        sessions.mkdir()
        (sessions / "ren").mkdir()

        backend = bridge.CodexBackend()

        # Track threads spawned
        thread_targets = []
        orig_thread_init = threading.Thread.__init__

        def track_thread(self, *a, **kw):
            orig_thread_init(self, *a, **kw)
            if kw.get("target"):
                thread_targets.append(kw["target"].__name__)

        # Local worker (no host in registry) — should use _codex_adapter_thread
        with patch.object(threading.Thread, "__init__", track_thread), \
             patch.object(threading.Thread, "start", lambda self: None):
            ok = backend.send("ren", "claude-prod-ren", "hello", "http://localhost:8271", sessions)
        assert ok is True, "send should return True"
        assert "_codex_adapter_thread" in thread_targets, f"expected local adapter thread, got {thread_targets}"

        # Remote worker — should use _codex_adapter_remote
        thread_targets.clear()
        bridge._registry_add("ren", "codex", 123, host="mac-mini")

        with patch.object(threading.Thread, "__init__", track_thread), \
             patch.object(threading.Thread, "start", lambda self: None):
            ok = backend.send("ren", "claude-prod-ren", "hello", "http://localhost:8271", sessions)
        assert ok is True, "remote send should return True"
        assert "_codex_adapter_remote" in thread_targets, f"expected remote adapter thread, got {thread_targets}"
    finally:
        bridge.NODE_DIR = orig_node
        bridge.WORKER_REGISTRY_FILE = orig_reg
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_codex_restart_readiness_check():
    from unittest.mock import patch
    import bridge

    # Non-interactive should verify tmux exists (not just return True)
    with patch.object(bridge, "tmux_exists", return_value=True):
        result = bridge._wait_for_restart_ready("claude-test-alice", "codex", timeout=2.0)
    assert result is True, f"expected True when tmux exists, got {result}"

    with patch.object(bridge, "tmux_exists", return_value=False):
        result = bridge._wait_for_restart_ready("claude-test-alice", "codex", timeout=2.0)
    assert result is False, f"expected False when tmux missing, got {result}"


def test_codex_progress_shows_last_response_time():
    import time
    import tempfile
    import shutil
    import os
    from pathlib import Path
    from unittest.mock import patch, MagicMock
    import bridge

    tmpdir = tempfile.mkdtemp()
    tmp = Path(tmpdir)
    orig_sessions = bridge.SESSIONS_DIR
    bridge.SESSIONS_DIR = tmp

    try:
        # Create worker with no codex transcript
        worker_dir = tmp / "alice"
        worker_dir.mkdir()

        # No adapter running, no transcript
        with patch.dict(bridge.processes.adapter_pids, {}, clear=True):
            activity = bridge._read_noninteractive_activity("alice")
        assert activity == "idle", f"expected idle with no transcript: {activity}"

        # With adapter running
        mock_proc = MagicMock()
        mock_proc.poll.return_value = None
        with patch.dict(bridge.processes.adapter_pids, {"alice": (mock_proc, None)}):
            activity = bridge._read_noninteractive_activity("alice")
        assert activity == "adapter running", f"expected adapter running: {activity}"

        # With recent transcript file
        transcript = tmp / "test.jsonl"
        transcript.write_text("{}")
        os.utime(transcript, (time.time() - 120, time.time() - 120))  # 2 min ago
        with patch.dict(bridge.processes.adapter_pids, {}, clear=True), \
             patch.object(bridge, "_find_codex_transcript", return_value=str(transcript)):
            activity = bridge._read_noninteractive_activity("alice")
        assert "2m ago" in activity, f"expected 2m ago in activity: {activity}"
    finally:
        bridge.SESSIONS_DIR = orig_sessions
        shutil.rmtree(tmpdir, ignore_errors=True)
