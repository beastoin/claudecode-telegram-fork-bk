"""Test fixtures for the split module architecture.

After splitting bridge.py into bridge, telegram, and claudecode,
tests that patch 'bridge.X' or 'claudecode.X' need the mock to be visible
in both the test's calling code AND the intra-module calls.

This conftest provides dual-patching to handle both cases.
"""
import pytest
import sys
from unittest.mock import patch


# Mutable globals that tests commonly override
_SYNCED_GLOBALS = [
    "SESSIONS_DIR", "WORKER_PIPE_ROOT", "NODE_DIR",
    "WORKER_REGISTRY_FILE", "TMUX_PREFIX", "PORT",
    "BRIDGE_URL", "NODE_NAME", "FILE_INBOX_ROOT",
    "BRIDGE_PUBLIC_URL", "BRIDGE_SSH_TARGET",
    "_subprocess_runner", "_clock",
    "transport", "telegram_api",
    "admin_chat_id",
]

_ALL_MODULES = ("bridge", "telegram", "claudecode")


@pytest.fixture(autouse=True)
def _sync_split_modules():
    """Restore mutable globals after each test."""
    modules = {}
    for name in _ALL_MODULES:
        try:
            modules[name] = __import__(name)
        except ImportError:
            pass

    snapshots = {}
    for gname in _SYNCED_GLOBALS:
        for mname, mod in modules.items():
            try:
                snapshots[(mname, gname)] = getattr(mod, gname)
            except AttributeError:
                pass

    yield

    for (mname, gname), val in snapshots.items():
        try:
            setattr(modules[mname], gname, val)
        except (AttributeError, TypeError):
            pass


def sync_global(name, value):
    """Set a mutable global across all split modules."""
    for mod_name in _ALL_MODULES:
        mod = sys.modules.get(mod_name)
        if mod and hasattr(mod, name):
            setattr(mod, name, value)
