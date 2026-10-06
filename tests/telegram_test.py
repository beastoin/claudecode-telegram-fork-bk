"""Tests for the telegram module — Telegram API, transport, and formatting.

Behavior tests: each test verifies something a user or the bridge
actually depends on, not that code structure exists.
"""
import telegram
import bridge

import os
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch


# ── IncomingMessage: the thing users send ────────────────────────────


def test_text_message_round_trip():
    """A plain text message parses into chat_id + text the router can use."""
    update = {
        "update_id": 1,
        "message": {
            "message_id": 10,
            "chat": {"id": 42},
            "text": "/hire alice",
        },
    }
    msg = telegram.IncomingMessage.from_update(update)
    assert msg.chat_id == 42
    assert msg.text == "/hire alice"
    assert msg.has_media is False
    assert msg.msg_id == 10


def test_photo_with_caption_parses():
    """Photo uploads carry caption as text and set has_media."""
    update = {
        "update_id": 2,
        "message": {
            "message_id": 11,
            "chat": {"id": 42},
            "caption": "screenshot of the bug",
            "photo": [{"file_id": "abc", "width": 800, "height": 600}],
        },
    }
    msg = telegram.IncomingMessage.from_update(update)
    assert msg.has_media is True
    assert msg.text == "screenshot of the bug"
    assert msg.photo is not None


def test_document_with_image_mime_detected():
    """Document sent as file with image/* mime is flagged for photo handling."""
    update = {
        "update_id": 3,
        "message": {
            "message_id": 12,
            "chat": {"id": 42},
            "document": {"file_id": "xyz", "mime_type": "image/png", "file_name": "chart.png"},
        },
    }
    msg = telegram.IncomingMessage.from_update(update)
    assert msg.doc_is_image is True
    assert msg.has_media is True


def test_reply_carries_original_context():
    """When user replies to a worker message, reply_to captures the original."""
    original = {"message_id": 5, "text": "alice: build is done"}
    update = {
        "update_id": 4,
        "message": {
            "message_id": 15,
            "chat": {"id": 42},
            "text": "deploy it",
            "reply_to_message": original,
        },
    }
    msg = telegram.IncomingMessage.from_update(update)
    assert msg.reply_to is not None
    assert msg.reply_to["message_id"] == 5


def test_empty_update_safe_defaults():
    """Malformed/empty updates don't crash — safe defaults everywhere."""
    msg = telegram.IncomingMessage.from_update({})
    assert msg.chat_id is None
    assert msg.text == ""
    assert msg.has_media is False


def test_media_group_carries_group_id():
    """Album (media_group) messages carry the group_id for batching."""
    update = {
        "update_id": 5,
        "message": {
            "message_id": 20,
            "chat": {"id": 42},
            "photo": [{"file_id": "p1", "width": 100, "height": 100}],
            "media_group_id": "grp-123",
        },
    }
    msg = telegram.IncomingMessage.from_update(update)
    assert msg.media_group_id == "grp-123"


# ── Message splitting: Telegram's 4096-char limit ────────────────────


def test_short_message_not_split():
    """Messages under 4096 chars go as one piece."""
    parts = telegram.split_message("hello world")
    assert parts == ["hello world"]


def test_long_message_split_within_limit():
    """Messages over 4096 chars get split, each part under the limit."""
    text = "line\n" * 2000  # ~10000 chars
    parts = telegram.split_message(text)
    assert len(parts) >= 2
    for part in parts:
        assert len(part) <= 4096


def test_split_preserves_all_content():
    """All words from original text appear across the split parts."""
    lines = [f"line_{i}" for i in range(200)]
    text = "\n".join(lines)
    parts = telegram.split_message(text)
    combined = "\n".join(parts)
    for line in lines:
        assert line in combined


# ── HTML escaping for Telegram ───────────────────────────────────────


def test_html_entities_escaped():
    """User text with <, >, & is escaped so Telegram doesn't parse it as HTML."""
    assert telegram.escape_html("if x < 3 && y > 5") == "if x &lt; 3 &amp;&amp; y &gt; 5"


def test_escape_idempotent_on_plain():
    """Plain text passes through unchanged."""
    assert telegram.escape_html("hello world") == "hello world"


# ── Markdown → Telegram HTML conversion ─────────────────────────────


def test_bold_converts_to_b_tags():
    """**bold** in worker output becomes <b>bold</b> for Telegram."""
    result = telegram.markdown_to_telegram_html("**important**")
    assert "<b>" in result and "important" in result


def test_inline_code_converts_to_code_tags():
    """`code` becomes <code>code</code>."""
    result = telegram.markdown_to_telegram_html("run `npm install`")
    assert "<code>" in result and "npm install" in result


def test_fenced_code_block_converts_to_pre():
    """Fenced code blocks become <pre> for monospace display."""
    md = "```python\ndef hello():\n    print('hi')\n```"
    result = telegram.markdown_to_telegram_html(md)
    assert "<pre>" in result and "def hello" in result


# ── File validation: security boundary ───────────────────────────────


def test_valid_photo_accepted(tmp_path):
    """A real JPEG file passes photo validation."""
    img = tmp_path / "photo.jpg"
    img.write_bytes(b"\xff\xd8\xff\xe0" + b"\x00" * 100)
    result = telegram.validate_photo_path(str(img))
    assert result.ok is True
    assert result.detail == img


def test_missing_photo_rejected():
    """Nonexistent file is rejected before any API call."""
    result = telegram.validate_photo_path("/nonexistent/photo.jpg")
    assert result.ok is False


def test_missing_document_rejected():
    """Nonexistent document file is rejected."""
    result = telegram.validate_document_path("/nonexistent/file.pdf")
    assert result.ok is False


def test_sensitive_filenames_blocked():
    """Files like .env and .netrc are blocked from upload."""
    assert telegram.is_blocked_filename(".env") is True
    assert telegram.is_blocked_filename(".netrc") is True
    assert telegram.is_blocked_filename("credentials") is True


def test_normal_filenames_allowed():
    """Regular files pass the blocklist check."""
    assert telegram.is_blocked_filename("readme.md") is False
    assert telegram.is_blocked_filename("report.pdf") is False


# ── Media tag parsing: worker→Telegram file sends ───────────────────


def test_image_tag_extracts_path_and_caption(tmp_path):
    """[[image:/path|caption]] in worker output is parsed for sendPhoto."""
    img = tmp_path / "chart.png"
    img.write_bytes(b"\x89PNG" + b"\x00" * 100)
    text = f"Here's the chart [[image:{img}|Monthly sales]]"
    remaining, tags = telegram.parse_image_tags(text)
    assert len(tags) == 1
    assert str(img) in tags[0][0]
    assert tags[0][1] == "Monthly sales"
    assert "[[image:" not in remaining


def test_file_tag_extracts_path_and_caption(tmp_path):
    """[[file:/path|caption]] in worker output is parsed for sendDocument."""
    doc = tmp_path / "report.pdf"
    doc.write_bytes(b"%PDF" + b"\x00" * 100)
    text = f"Report ready [[file:{doc}|Q4 report]]"
    remaining, tags = telegram.parse_file_tags(text)
    assert len(tags) == 1
    assert str(doc) in tags[0][0]
    assert tags[0][1] == "Q4 report"


# ── Persistence: survive bridge restarts ─────────────────────────────


def test_chat_id_survives_restart(tmp_path):
    """Last chat ID round-trips through save→load (survives restart)."""
    bridge.NODE_DIR = tmp_path
    telegram.LAST_CHAT_ID_FILE = tmp_path / "last_chat_id"
    telegram.save_last_chat_id(98765)
    assert telegram.load_last_chat_id() == 98765


def test_active_worker_survives_restart(tmp_path):
    """Last active worker name round-trips (auto-focus on restart)."""
    bridge.NODE_DIR = tmp_path
    telegram.LAST_ACTIVE_FILE = tmp_path / "last_active"
    telegram.save_last_active("alice")
    assert telegram.load_last_active() == "alice"


def test_missing_chat_id_returns_none(tmp_path):
    """Before any message, load_last_chat_id returns None (no crash)."""
    telegram.LAST_CHAT_ID_FILE = tmp_path / "nonexistent"
    assert telegram.load_last_chat_id() is None


# ── Format response: strip worker name prefix ───────────────────────


def test_worker_name_stripped_from_output():
    """Worker output 'alice: done' becomes 'done' for Telegram display."""
    result = telegram.format_response_text("alice", "alice: task complete")
    assert not result.startswith("alice:")
    assert "task complete" in result


# ── File size formatting ─────────────────────────────────────────────


def test_file_size_human_readable():
    """Byte counts display as human-readable KB/MB."""
    assert telegram.format_file_size(0) == "0 B"
    assert "KB" in telegram.format_file_size(1500)
    assert "MB" in telegram.format_file_size(2_000_000)


# ── LocalTransport: testing without Telegram API ─────────────────────


def test_local_transport_returns_ok():
    """LocalTransport simulates success for offline testing."""
    t = telegram.LocalTransport()
    result = t.send_text(123, "test message")
    assert result["ok"] is True


def test_local_transport_writes_log_file(tmp_path):
    """With TRANSPORT_LOG set, LocalTransport logs each call to a file."""
    log_file = tmp_path / "transport.log"
    os.environ["TRANSPORT_LOG"] = str(log_file)
    try:
        t = telegram.LocalTransport()
        t.send_text(42, "hello")
        t.send_photo(42, "/img.jpg")
        content = log_file.read_text()
        assert "send_text" in content
        assert "send_photo" in content
    finally:
        del os.environ["TRANSPORT_LOG"]


# ── Split architecture: proxy propagation ────────────────────────────


def test_mock_on_bridge_reaches_telegram():
    """Setting bridge.admin_chat_id propagates to telegram module.

    This is the core split-architecture invariant: tests that set
    bridge.X must affect telegram functions that read X.
    """
    original = telegram.admin_chat_id
    try:
        bridge.admin_chat_id = 99999
        assert telegram.admin_chat_id == 99999
    finally:
        bridge.admin_chat_id = original
