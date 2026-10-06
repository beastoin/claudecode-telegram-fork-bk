"""Telegram Bot API, transport, and message formatting.

Handles all communication with the Telegram Bot API: sending and receiving
messages, media handling, HTML sanitization, and message splitting.

Independently importable — no bridge.py dependency at module level.
Imports infrastructure from core.py (logging, DI seams, config).
Owns all Telegram domain types (ChatId, TelegramMessageDict, etc.).
"""

from __future__ import annotations

# ── Infrastructure from core (no circular dependency) ──────────────────
from core import (
    _str_field, _int_field, _dict_field, _bool_field,
    _log, _LOG_ERROR, _LOG_WARN, _LOG_INFO, _LOG_DEBUG,
    _log_best_effort,
    SubprocessRunner, Clock, MarkdownToken,
    _subprocess_runner, _urlopen,
    AppContext, get_app_context,
    VERSION,
    BOT_TOKEN, NODE_NAME, NODE_DIR,
    SESSIONS_DIR, TIMEOUT_HTTP_API, TIMEOUT_HTTP_DOWNLOAD, TIMEOUT_HTTP_UPLOAD,
    TIMEOUT_PROCESS_WAIT, TIMEOUT_FILE_TRANSFER, TIMEOUT_TMUX_CHECK,
    PENDING_TIMEOUT,
    STT_ENDPOINT, STT_TIMEOUT,
    TEAM_DIR,
    ADMIN_CHAT_ID_ENV, admin_chat_id,
    DEFAULT_BACKEND,
)

import collections
from dataclasses import dataclass, field
import hashlib
import http.client
import os
import json
import mimetypes
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import types
import uuid
import urllib.error
import urllib.request
from html.parser import HTMLParser
from pathlib import Path
from collections.abc import Iterable, Mapping
from typing import IO, Any, Callable, Iterator, Literal, NamedTuple, Protocol, TypedDict, TYPE_CHECKING, cast, runtime_checkable


# ── Telegram domain types (owned by this module) ─────────────────────

ChatId = int | str

MessageId = int

ParseMode = Literal["HTML", "MarkdownV2"] | None


class TelegramApiResponseDict(TypedDict, total=False):
    """Shape of a Telegram Bot API JSON response."""
    ok: bool
    result: object  # varies by method — Message, User, bool, etc.
    description: str
    error_code: int


TelegramApiResponse = TelegramApiResponseDict | None


class TelegramUser(TypedDict, total=False):
    """Telegram User object fields."""
    id: int
    is_bot: bool
    first_name: str
    last_name: str
    username: str


class TelegramChat(TypedDict, total=False):
    """Telegram Chat object fields."""
    id: int
    type: str
    title: str
    username: str


class TelegramPhotoSize(TypedDict, total=False):
    """Telegram PhotoSize object fields."""
    file_id: str
    file_unique_id: str
    width: int
    height: int
    file_size: int


class TelegramDocument(TypedDict, total=False):
    """Telegram Document object fields."""
    file_id: str
    file_unique_id: str
    file_name: str
    mime_type: str
    file_size: int


class TelegramVoice(TypedDict, total=False):
    """Telegram Voice object fields."""
    file_id: str
    file_unique_id: str
    duration: int
    mime_type: str
    file_size: int


class TelegramVideo(TypedDict, total=False):
    """Telegram Video object fields."""
    file_id: str
    file_unique_id: str
    width: int
    height: int
    duration: int
    file_name: str
    mime_type: str
    file_size: int


class TelegramAudio(TypedDict, total=False):
    """Telegram Audio object fields."""
    file_id: str
    file_unique_id: str
    duration: int
    performer: str
    title: str
    file_name: str
    mime_type: str
    file_size: int


class TelegramSticker(TypedDict, total=False):
    """Telegram Sticker object fields."""
    file_id: str
    file_unique_id: str
    width: int
    height: int
    emoji: str
    type: str
    is_animated: bool
    is_video: bool


class TelegramMessageDict(TypedDict, total=False):
    """Telegram Message object fields (raw JSON from API)."""
    message_id: int
    chat: TelegramChat
    date: int
    text: str
    photo: list[TelegramPhotoSize]
    document: TelegramDocument
    voice: TelegramVoice
    video: TelegramVideo
    video_note: TelegramVideo
    animation: TelegramDocument
    audio: TelegramAudio
    sticker: TelegramSticker
    reply_to_message: "TelegramMessageDict"
    caption: str
    media_group_id: str
    rich_message: dict[str, str]  # {"markdown": str} — Telegram rich message block


# Note: Telegram API uses "from" (a Python keyword), so we use functional TypedDict form.
TelegramCallbackQuery = TypedDict("TelegramCallbackQuery", {
    "id": str,
    "from": TelegramUser,
    "message": TelegramMessageDict,
    "data": str,
}, total=False)


class TelegramUpdate(TypedDict, total=False):
    """Telegram Update object fields (raw webhook payload)."""
    update_id: int
    message: TelegramMessageDict
    edited_message: TelegramMessageDict
    callback_query: TelegramCallbackQuery


class FileValidation(NamedTuple):
    """Result of validating a file path (photo or document)."""
    ok: bool
    detail: Path | str  # Path on success, error message on failure


class MediaGroupEntry(TypedDict):
    """Buffered media group state during collection."""
    items: list[TelegramMessageDict]
    caption: str
    timer: threading.Timer | None


# ── Cross-module type annotations (TYPE_CHECKING only) ───────────────
# WorkerStateEntry and TmuxSessionDict are claudecode-owned types used
# in function signatures here. With `from __future__ import annotations`,
# all annotations are strings at runtime — no circular import.
if TYPE_CHECKING:
    from claudecode import WorkerStateEntry, TmuxSessionDict



# ── IncomingMessage: parse Telegram update once ──

@dataclass
class IncomingMessage:
    """Parsed Telegram update — created once, read everywhere."""
    update_id: int = 0
    chat_id: int | None = None
    msg_id: int | None = None
    text: str = ""
    # Media fields (Telegram API JSON shapes)
    photo: list[TelegramPhotoSize] | None = None
    document: TelegramDocument | None = None
    animation: TelegramDocument | None = None
    audio: TelegramAudio | None = None
    voice: TelegramVoice | None = None
    video: TelegramVideo | None = None
    video_note: TelegramVideo | None = None
    sticker: TelegramSticker | None = None
    # Derived
    has_media: bool = False
    doc_is_image: bool = False
    media_group_id: str | None = None
    # Reply context (Telegram API reply_to_message JSON)
    reply_to: TelegramMessageDict | None = None
    # Raw message dict (for edge cases during migration)
    raw_msg: TelegramMessageDict = field(default_factory=lambda: TelegramMessageDict())

    @classmethod
    def from_update(cls: type["IncomingMessage"], update: TelegramUpdate) -> "IncomingMessage":
        """Factory: parse a Telegram update dict into an IncomingMessage."""
        msg = update.get("message", {})
        text = msg.get("text", "") or msg.get("caption", "")
        photo = msg.get("photo")
        document = msg.get("document")
        animation = msg.get("animation")
        audio = msg.get("audio")
        voice = msg.get("voice")
        video = msg.get("video")
        video_note = msg.get("video_note")
        sticker = msg.get("sticker")

        doc_is_image = False
        if document:
            mime_type = document.get("mime_type", "")
            doc_is_image = mime_type.startswith("image/")

        has_media = bool(
            photo or document or animation or video
            or audio or voice or video_note or sticker
        )

        return cls(
            update_id=update.get("update_id", 0),
            chat_id=msg.get("chat", {}).get("id"),
            msg_id=msg.get("message_id"),
            text=text,
            photo=photo,
            document=document,
            animation=animation,
            audio=audio,
            voice=voice,
            video=video,
            video_note=video_note,
            sticker=sticker,
            has_media=has_media,
            doc_is_image=doc_is_image,
            media_group_id=msg.get("media_group_id"),
            reply_to=msg.get("reply_to_message"),
            raw_msg=msg,
        )



def _extract_msg_text(msg: TelegramMessageDict) -> str:
    """Extract plain text from a Telegram message, including rich_message blocks."""
    text = msg.get("text") or msg.get("caption") or ""
    if not text:
        rich = msg.get("rich_message")
        blocks = rich.get("blocks") if rich else None
        if isinstance(blocks, list):
            parts = []
            for b in blocks:
                if not isinstance(b, dict):
                    continue
                bt = b.get("text")
                if not bt:
                    continue
                if isinstance(bt, str):
                    parts.append(bt)
                elif isinstance(bt, list):
                    parts.append("".join(
                        chunk if isinstance(chunk, str) else chunk.get("text", "")
                        for chunk in bt
                    ))
            text = "\n".join(parts)
    return text


def _build_cwd_change_notice(
    name: str,
    old_cwd: str,
    new_cwd: str,
    old_sid: str,
) -> str:
    """Build a Telegram notification for a worker CWD change.

    Tells the manager where the worker was, where it's going, and
    whether a previous session was discarded.
    """
    lines = [f"⚠️ {name}: workspace changed"]
    if old_sid:
        lines.append(f"<b>from:</b> <code>{old_cwd}</code> (session <code>{old_sid[:12]}…</code>)")
    else:
        lines.append(f"<b>from:</b> <code>{old_cwd}</code> (no previous session)")
    lines.append(f"<b>to:</b> <code>{new_cwd}</code> (fresh start)")
    return "\n".join(lines)


# ── Team formatting (pure functions, runtime state injected by caller) ──


def _normalize_activity(raw: str) -> str:
    """Normalize Claude Code spinner verbs to human-friendly text.

    Claude Code TUI shows random verbs like "Ionizing", "Hullaballooing",
    "Schlepping" as thinking spinner text. These are meaningless to managers.
    Normalize single-word spinner verbs to "Thinking (duration)".

    Multi-word activities like "Running Bash", "Compacting conversation",
    "In plan mode", etc. pass through unchanged.
    """
    if not raw:
        return raw
    # Pattern: single capitalized gerund word optionally followed by (duration)
    m = re.match(r'^([A-Z][a-z]+ing)\s*(?:\((.+)\))?\s*$', raw)
    if m:
        verb = m.group(1)
        dur = m.group(2)
        # Known multi-word prefixes that happen to start with a gerund are handled
        # by the regex requiring the FULL string to be one word + optional duration.
        # "Running Bash" won't match because "Bash" follows after a space.
        # "Compacting conversation (5m)" won't match because "conversation" follows.
        # Only single-word verbs like "Ionizing", "Whirring" match.
        if dur:
            return f"Thinking ({dur})"
        return "Thinking"
    return raw


def _team_attention_summary(watchdog_status: str, activity: str) -> tuple[str, str, int]:
    """Return (icon, blocker_label, sort_rank) for /team rows."""
    status = (watchdog_status or "").lower()
    act = (activity or "").lower()

    if "rate limit" in act:
        return "🔴", "rate limit", 0
    if "error" in act or "traceback" in act or "not running" in act or "failed" in act:
        return "🔴", "error", 0
    if "needs input" in status or "needs reply" in status:
        return "🟡", "needs reply", 1
    if "stuck" in status or "no progress" in status:
        return "🔴", "stuck", 0
    if "poisoned" in status or "error loop" in status:
        return "🔴", "error loop", 0
    if "dead" in status or "not responding" in status:
        return "🔴", "stopped", 0
    if "offline" in status:
        return "🔴", "offline", 0
    if "exited" in status or "session ended" in status:
        return "🔴", "session ended", 0

    waiting_signals = (
        "waiting for",
        "awaiting",
        "approval",
        "accept edits",
        "confirm",
        "in plan mode",
    )
    if "working (waiting)" in status or any(sig in act for sig in waiting_signals):
        return "🟡", "needs reply", 1

    return "🟢", "ok", 2


def _format_watchdog_status(name: str,
                            pending_lookup: 'Callable[[str], bool] | None' = None,
                            state_snapshot: 'dict[str, WorkerStateEntry] | None' = None,
                            clock_now: float | None = None) -> str:
    """Format a human-readable watchdog status line for one worker.

    Pure function — all state injected via params. The claudecode.py wrapper
    provides defaults from runtime globals (watchdog, is_pending, _clock).
    """
    if pending_lookup is None:
        pending_lookup = lambda _: False  # noqa: E731
    if clock_now is None:
        clock_now = time.time()

    entry = state_snapshot.get(name) if state_snapshot else None
    if not entry:
        return "Working" if pending_lookup(name) else "Ready"

    state, _reason, since = entry
    now = clock_now

    if state == "READY":
        return "Ready"
    if state == "BUSY_TOOL":
        return "Working"
    if state == "BUSY_THINKING":
        return "Thinking"
    if state == "WAITING":
        return "Working"
    if state == "WAITING_INPUT":
        minutes = max(0, int((now - since) / 60)) if since else 0
        return f"Needs reply ({minutes}m)"
    if state == "STUCK":
        # Use age from reason (derived from pending file timestamp on disk,
        # survives bridge restarts) rather than since (resets on restart).
        age_match = re.search(r"age=(\d+)s", _reason) if _reason else None
        if age_match:
            minutes = int(age_match.group(1)) // 60
        else:
            minutes = max(0, int((now - since) / 60)) if since else 0
        return f"No progress ({minutes}m)"
    if state == "POISONED":
        minutes = max(0, int((now - since) / 60)) if since else 0
        return f"Error loop ({minutes}m)"
    if state == "DEAD":
        return "Not responding"
    if state == "HOST_OFFLINE":
        return "Host offline"
    if state == "OFFLINE":
        return "Offline"
    if state == "EXITED":
        return "Session ended"
    if state == "UNTRACKED_BUSY":
        return "Working"
    return state.lower()


def format_team_lines(
    registered: 'dict[str, TmuxSessionDict]',
    active: str | None,
    pending_lookup: 'Callable[[str], bool] | None' = None,
    worker_live: 'dict[str, TmuxSessionDict] | dict[str, dict[str, str | None]] | None' = None,
    *,
    state_snapshot: 'dict[str, WorkerStateEntry] | None' = None,
    clock_now: float | None = None,
    normalize_backend_fn: 'Callable[[str | None], str] | None' = None,
) -> list[str]:
    """Format /team response lines with attention, activity, and context.

    Pure function — all state injected via params. The claudecode.py wrapper
    provides defaults from runtime globals (watchdog, is_pending, _clock,
    normalize_backend).
    """
    if pending_lookup is None:
        pending_lookup = lambda _: False  # noqa: E731
    if worker_live is None:
        worker_live = {}
    if state_snapshot is None:
        state_snapshot = {}
    if clock_now is None:
        clock_now = time.time()
    if normalize_backend_fn is None:
        normalize_backend_fn = lambda b: b or DEFAULT_BACKEND  # noqa: E731

    backend_values = set()
    for name, session in registered.items():
        live = worker_live.get(name, {})
        backend = normalize_backend_fn(live.get("backend") or session.get("backend"))
        backend_values.add(backend)
    show_backend = len(backend_values) > 1

    rows = []
    counts = {"🔴": 0, "🟡": 0, "🟢": 0}
    for name in sorted(registered.keys()):
        session = registered[name]
        watchdog_status = _format_watchdog_status(name, pending_lookup,
                                                  state_snapshot=state_snapshot,
                                                  clock_now=clock_now)
        live = worker_live.get(name, {})
        backend = normalize_backend_fn(live.get("backend") or session.get("backend"))

        raw_activity = str(live.get("activity") or "").strip()
        if not raw_activity or raw_activity == "Unknown":
            raw_activity = watchdog_status
        activity = _normalize_activity(raw_activity)
        if len(activity) > 42:
            activity = activity[:39].rstrip() + "..."

        context_pct = str(live.get("context_pct") or "").strip()
        icon, blocker, severity_rank = _team_attention_summary(watchdog_status, raw_activity)
        counts[icon] += 1

        name_cell = f"{name} 🎯" if name == active else name
        ctx_part = f" | ctx {context_pct}" if context_pct and context_pct != "--" else ""
        row = f"{icon} {name_cell} — {activity}{ctx_part}"
        if show_backend:
            row += f" | backend={backend}"

        focus_rank = 0 if name == active else 1
        rows.append((severity_rank, focus_rank, name, blocker, row))

    rows.sort(key=lambda item: (item[0], item[1], item[2]))
    attention_rows = [f"{name} ({blocker})" for rank, _focus, name, blocker, _row in rows if rank < 2]

    lines = []
    focused = active or "(none)"
    lines.append(
        f"Team: {len(registered)} agents · focused: {focused} | "
        f"🟢 {counts['🟢']} ok · 🟡 {counts['🟡']} need reply · 🔴 {counts['🔴']} blocked"
    )
    if attention_rows:
        lines.append("Needs your reply: " + ", ".join(attention_rows))
    lines.extend(row for _rank, _focus, _name, _blocker, row in rows)
    return lines


# admin_chat_id lives in bridge (shared by telegram + claudecode)

LAST_CHAT_ID_FILE = NODE_DIR / "last_chat_id"

LAST_ACTIVE_FILE = NODE_DIR / "last_active"



class MediaGroupState:
    """Buffer for Telegram media groups — collects items before routing."""

    def __init__(self) -> None:
        """Initialize media group buffer and collection lock."""
        self.buffer: dict[str, MediaGroupEntry] = {}
        self.lock: threading.Lock = threading.Lock()

_MEDIA_GROUP_WAIT: float = 0.8  # seconds to wait for all items in a group


BOT_COMMANDS = [
    # Daily commands (frequency-first, natural workflow order)
    {"command": "team", "description": "Show your team"},
    {"command": "focus", "description": "Focus a worker: /focus <name>"},
    {"command": "restart", "description": "Restart worker (--clean for fresh)"},
    # Occasional
    {"command": "settings", "description": "Show settings"},
    {"command": "pilot", "description": "Toggle pilot access: /pilot <name>"},
    {"command": "relay", "description": "Open public channel: /relay <worker>"},
    {"command": "rewind", "description": "Transcript viewer: /rewind <name>"},
    {"command": "pr", "description": "PR review viewer: /pr <github_pr_url>"},
    # Rare (onboarding/offboarding)
    {"command": "hire", "description": "Hire a worker: /hire <name>"},
    {"command": "end", "description": "Offboard a worker: /end <name>"},
]


BLOCKED_COMMANDS = [
    "/mcp", "/help", "/config", "/model", "/compact", "/cost",
    "/doctor", "/init", "/login", "/logout", "/permissions",
    "/pr", "/review", "/terminal", "/vim", "/approved-tools", "/listen"
]



# ============================================================
# FILE PERSISTENCE
# ============================================================

# ─────────────────────────────────────────────────────────────────────────────
# Persistence (last chat ID and last active worker survive restart)
# ─────────────────────────────────────────────────────────────────────────────

def save_last_chat_id(chat_id: ChatId | None) -> None:
    """Save last known chat ID to file for auto-notification on restart."""
    if chat_id is None:
        return
    try:
        NODE_DIR.mkdir(parents=True, exist_ok=True, mode=0o700)
        _tmp = LAST_CHAT_ID_FILE.with_suffix('.tmp')
        _tmp.write_text(str(chat_id))
        _tmp.chmod(0o600)
        os.replace(str(_tmp), str(LAST_CHAT_ID_FILE))
    except OSError as e:
        _log(_LOG_WARN, "bridge", f"Failed to save last_chat_id: {e}")



def load_last_chat_id() -> int | None:
    """Load last known chat ID from file."""
    try:
        if LAST_CHAT_ID_FILE.exists():
            chat_id = LAST_CHAT_ID_FILE.read_text().strip()
            if chat_id:
                return int(chat_id)
    except OSError as e:
        _log(_LOG_WARN, "bridge", f"Failed to load last_chat_id: {e}")
    return None



def save_last_active(name: str) -> None:
    """Save last active worker name to file for auto-focus on restart."""
    try:
        NODE_DIR.mkdir(parents=True, exist_ok=True, mode=0o700)
        _tmp = LAST_ACTIVE_FILE.with_suffix('.tmp')
        _tmp.write_text(name)
        _tmp.chmod(0o600)
        os.replace(str(_tmp), str(LAST_ACTIVE_FILE))
    except OSError as e:
        _log(_LOG_WARN, "bridge", f"Failed to save last_active: {e}")



def load_last_active() -> str | None:
    """Load last active worker name from file."""
    try:
        if LAST_ACTIVE_FILE.exists():
            name = LAST_ACTIVE_FILE.read_text().strip()
            if name:
                return name
    except OSError as e:
        _log(_LOG_WARN, "bridge", f"Failed to load last_active: {e}")
    return None



# ============================================================
# MESSAGE TRANSPORT ABSTRACTION
# ============================================================

TRANSPORT_MODE = os.environ.get("TRANSPORT", "telegram")



@runtime_checkable
class MessageTransport(Protocol):
    """Protocol for all outbound messaging from bridge to manager.

    All methods are fully type-annotated so static checkers can verify
    that transports and callers agree on argument/return types.
    Uses Protocol (not ABC) so test mocks satisfy structural subtyping.
    """

    @property
    def name(self) -> str:
        """Return the transport name identifier."""
        ...

    def send_text(self, chat_id: ChatId, text: str,
                  parse_mode: ParseMode = None,
                  reply_to: MessageId | None = None) -> TelegramApiResponse:
        """Send a plain text message."""
        ...

    def send_rich_text(self, chat_id: ChatId, markdown: str,
                       reply_to: MessageId | None = None) -> TelegramApiResponse:
        """Send a rich-formatted (markdown) text message."""
        ...

    def send_photo(self, chat_id: ChatId, photo_path: str | Path,
                   caption: str | None = None) -> bool:
        """Send a photo to a Telegram chat."""
        ...

    def send_document(self, chat_id: ChatId, doc_path: str | Path,
                      caption: str | None = None) -> bool:
        """Send a document file to a Telegram chat."""
        ...

    def send_animation(self, chat_id: ChatId, animation_path: str | Path,
                       caption: str | None = None) -> bool:
        """Send an animation (GIF/MP4) to a Telegram chat."""
        ...

    def send_video(self, chat_id: ChatId, video_path: str | Path,
                   caption: str | None = None) -> bool:
        """Send a video to a Telegram chat."""
        ...

    def send_audio(self, chat_id: ChatId, audio_path: str | Path,
                   caption: str | None = None) -> bool:
        """Send an audio file to a Telegram chat."""
        ...

    def send_voice(self, chat_id: ChatId, voice_path: str | Path,
                   caption: str | None = None) -> bool:
        """Send a voice message to a Telegram chat."""
        ...

    def send_sticker(self, chat_id: ChatId, sticker_path: str | Path) -> bool:
        """Send a sticker to a Telegram chat."""
        ...

    def send_chat_action(self, chat_id: ChatId, action: str) -> None:
        """Send a chat action indicator (typing, uploading, etc.)."""
        ...

    def set_reaction(self, chat_id: ChatId, message_id: MessageId,
                     reaction: list[dict[str, str]]) -> None:
        """Set an emoji reaction on a message."""
        ...

    def edit_message(self, chat_id: ChatId, message_id: MessageId,
                     text: str, parse_mode: ParseMode = None) -> TelegramApiResponse:
        """Edit an existing message by its ID."""
        ...

    def setup_commands(self, commands: list[dict[str, str]]) -> None:
        """Register bot command suggestions with Telegram."""
        ...

    def download_file(self, file_id: str, session_name: str) -> str | None:
        """Download a file from Telegram by file ID."""
        ...



# ============================================================
# TELEGRAM API
# ============================================================

class TelegramAPI:
    """Low-level Telegram Bot API caller. Fully type-annotated."""

    def __init__(self, token: str) -> None:
        """Initialize Telegram Bot API client with the given token."""
        self.token: str = token

    def api(self, method: str, data: Mapping[str, object] | dict[str, object]) -> TelegramApiResponse:
        """Make a raw Telegram Bot API call and return the response."""
        if not self.token:
            return None
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{self.token}/{method}",
            data=json.dumps(data).encode(),
            headers={"Content-Type": "application/json"}
        )
        try:
            with _urlopen(req, timeout=TIMEOUT_HTTP_API) as r:
                return cast(TelegramApiResponseDict, json.loads(r.read()))
        except urllib.error.HTTPError as e:
            _log(_LOG_ERROR, "telegram", f"Telegram API error: {e}")
            try:
                raw = e.read()
                body = cast(TelegramApiResponseDict, json.loads(raw))
                return body  # Return error response so callers can inspect description
            except (urllib.error.URLError, OSError, TimeoutError, json.JSONDecodeError, ValueError):
                # Non-JSON error body (proxy, middlebox, empty) — return structured error
                return {"ok": False, "error_code": e.code, "description": f"HTTP {e.code} (non-JSON body)"}
        except (json.JSONDecodeError, KeyError, ValueError, TypeError) as e:
            _log(_LOG_ERROR, "telegram", f"Telegram API error: {e}")
            return None

    def send_message(self, chat_id: ChatId, text: str, **kwargs: object) -> TelegramApiResponse:
        """Send a text message via the Telegram Bot API."""
        payload: dict[str, object] = {"chat_id": chat_id, "text": text}
        payload.update(kwargs)
        return self.api("sendMessage", payload)

    def send_rich_message(self, chat_id: ChatId, markdown: str, **kwargs: object) -> TelegramApiResponse:
        """Send a rich-formatted message via the Telegram Bot API."""
        payload: dict[str, object] = {
            "chat_id": chat_id,
            "rich_message": {"markdown": markdown},
        }
        payload.update(kwargs)
        return self.api("sendRichMessage", payload)

    def send_photo(self, chat_id: ChatId, photo: str, **kwargs: object) -> TelegramApiResponse:
        """Send a photo to a Telegram chat."""
        payload: dict[str, object] = {"chat_id": chat_id, "photo": photo}
        payload.update(kwargs)
        return self.api("sendPhoto", payload)

    def send_document(self, chat_id: ChatId, document: str, **kwargs: object) -> TelegramApiResponse:
        """Send a document file to a Telegram chat."""
        payload: dict[str, object] = {"chat_id": chat_id, "document": document}
        payload.update(kwargs)
        return self.api("sendDocument", payload)

    def send_animation(self, chat_id: ChatId, animation: str, **kwargs: object) -> TelegramApiResponse:
        """Send an animation (GIF/MP4) to a Telegram chat."""
        payload: dict[str, object] = {"chat_id": chat_id, "animation": animation}
        payload.update(kwargs)
        return self.api("sendAnimation", payload)

    def set_reaction(self, chat_id: ChatId, message_id: MessageId,
                     reaction: list[dict[str, str]]) -> TelegramApiResponse:
        """Set an emoji reaction on a message."""
        payload: dict[str, object] = {"chat_id": chat_id, "message_id": message_id, "reaction": reaction}
        return self.api("setMessageReaction", payload)

    def send_chat_action(self, chat_id: ChatId, action: str) -> TelegramApiResponse:
        """Send a chat action indicator (typing, uploading, etc.)."""
        return self.api("sendChatAction", {"chat_id": chat_id, "action": action})



class TelegramTransport(MessageTransport):
    """Transport that sends messages via Telegram Bot API."""

    def __init__(self, token: str) -> None:
        """Initialize Telegram transport wrapping a TelegramAPI instance."""
        self._api: TelegramAPI = TelegramAPI(token)

    @property
    def name(self) -> str:
        """Return the transport name identifier."""
        return "telegram"

    def send_text(self, chat_id: ChatId, text: str,
                  parse_mode: ParseMode = None,
                  reply_to: MessageId | None = None) -> TelegramApiResponse:
        """Send a plain text message."""
        payload = {"chat_id": chat_id, "text": text}
        if parse_mode:
            payload["parse_mode"] = parse_mode
        if reply_to:
            payload["reply_to_message_id"] = reply_to
        # Use module-level telegram_api so tests can mock bridge.telegram_api
        return telegram_api("sendMessage", payload)

    def send_rich_text(self, chat_id: ChatId, markdown: str,
                       reply_to: MessageId | None = None) -> TelegramApiResponse:
        """Send a rich-formatted (markdown) text message."""
        payload: dict[str, object] = {
            "chat_id": chat_id,
            "rich_message": {"markdown": markdown},
        }
        if reply_to:
            payload["reply_to_message_id"] = reply_to
        return telegram_api("sendRichMessage", payload)

    def send_photo(self, chat_id: ChatId, photo_path: str | Path,
                   caption: str | None = None) -> bool:
        """Send a photo to a Telegram chat.

        Validates and auto-resizes the photo if oversized before sending.
        """
        if not BOT_TOKEN:
            return False
        ok, validated = validate_photo_path(photo_path)
        if not ok:
            _log(_LOG_WARN, "telegram", validated)
            return False
        photo_data, filename = _prepare_photo_for_telegram(validated)
        mime = mimetypes.guess_type(str(validated))[0] or "image/jpeg"
        return self._send_media_multipart(
            chat_id, validated, "photo", "sendPhoto", caption,
            file_data=photo_data, filename=filename, mime_type=mime,
        )

    def send_animation(self, chat_id: ChatId, animation_path: str | Path,
                       caption: str | None = None) -> bool:
        """Send an animation (GIF/MP4) to a Telegram chat."""
        if not BOT_TOKEN:
            return False
        ok, validated = validate_photo_path(animation_path)
        if not ok:
            _log(_LOG_WARN, "telegram", validated)
            return False
        mime = "video/mp4" if Path(validated).suffix.lower() == ".mp4" else "image/gif"
        return self._send_media_multipart(
            chat_id, validated, "animation", "sendAnimation", caption,
            mime_type=mime,
        )

    def send_document(self, chat_id: ChatId, doc_path: str | Path,
                      caption: str | None = None) -> bool:
        """Send a document file to a Telegram chat."""
        if not BOT_TOKEN:
            return False
        ok, validated = validate_document_path(doc_path)
        if not ok:
            _log(_LOG_WARN, "telegram", validated)
            return False
        return self._send_media_multipart(
            chat_id, validated, "document", "sendDocument", caption,
        )

    def _send_media_multipart(self, chat_id: ChatId, file_path: Path | str,
                              field_name: str, api_method: str,
                              caption: str | None = None,
                              file_data: bytes | None = None,
                              filename: str | None = None,
                              mime_type: str | None = None) -> bool:
        """Send a file to Telegram using multipart/form-data.

        Args:
            chat_id: Target chat.
            file_path: Path to the file (used for data, filename, and MIME
                type unless overridden by the optional parameters).
            field_name: Telegram API field name (e.g. "photo", "document").
            api_method: Telegram Bot API method (e.g. "sendPhoto").
            caption: Optional caption text.
            file_data: Pre-loaded bytes (skips reading file_path if provided).
            filename: Override for the filename in Content-Disposition.
            mime_type: Override for the Content-Type of the file part.

        Returns:
            True on success, False on failure.
        """
        if not BOT_TOKEN:
            return False
        file_path = Path(file_path)
        data = file_data if file_data is not None else file_path.read_bytes()
        fname = filename or file_path.name
        ctype = mime_type or mimetypes.guess_type(str(file_path))[0] or "application/octet-stream"
        boundary = uuid.uuid4().hex
        body_parts: list[bytes] = [
            f"--{boundary}".encode(),
            b'Content-Disposition: form-data; name="chat_id"',
            b"",
            str(chat_id).encode(),
            f"--{boundary}".encode(),
            f'Content-Disposition: form-data; name="{field_name}"; filename="{fname}"'.encode(),
            f"Content-Type: {ctype}".encode(),
            b"",
            data,
        ]
        if caption:
            body_parts.extend([
                f"--{boundary}".encode(),
                b'Content-Disposition: form-data; name="caption"',
                b"",
                caption.encode(),
            ])
        body_parts.append(f"--{boundary}--".encode())
        body_parts.append(b"")
        body = b"\r\n".join(body_parts)
        try:
            req = urllib.request.Request(
                f"https://api.telegram.org/bot{BOT_TOKEN}/{api_method}",
                data=body,
                headers={"Content-Type": f"multipart/form-data; boundary={boundary}"}
            )
            with _urlopen(req, timeout=TIMEOUT_HTTP_UPLOAD) as r:
                result = cast(TelegramApiResponseDict, json.loads(r.read()))
                if result.get("ok"):
                    _log(_LOG_INFO, "telegram", f"{api_method} sent: {fname}")
                    return True
                else:
                    _log(_LOG_WARN, "bridge", f"{api_method} failed: {result}")
                    return False
        except (urllib.error.URLError, OSError, TimeoutError) as e:
            _log(_LOG_ERROR, "bridge", f"{api_method} error: {e}")
            return False

    def send_video(self, chat_id: ChatId, video_path: str | Path,
                   caption: str | None = None) -> bool:
        """Send a video to a Telegram chat."""
        ok, validated = validate_document_path(video_path)
        if not ok:
            _log(_LOG_WARN, "telegram", validated)
            return False
        return self._send_media_multipart(chat_id, validated, "video", "sendVideo", caption)

    def send_audio(self, chat_id: ChatId, audio_path: str | Path,
                   caption: str | None = None) -> bool:
        """Send an audio file to a Telegram chat."""
        ok, validated = validate_document_path(audio_path)
        if not ok:
            _log(_LOG_WARN, "telegram", validated)
            return False
        return self._send_media_multipart(chat_id, validated, "audio", "sendAudio", caption)

    def send_voice(self, chat_id: ChatId, voice_path: str | Path,
                   caption: str | None = None) -> bool:
        """Send a voice message to a Telegram chat."""
        ok, validated = validate_document_path(voice_path)
        if not ok:
            _log(_LOG_WARN, "telegram", validated)
            return False
        return self._send_media_multipart(chat_id, validated, "voice", "sendVoice", caption)

    def send_sticker(self, chat_id: ChatId, sticker_path: str | Path) -> bool:
        """Send a sticker to a Telegram chat."""
        sticker_path = Path(sticker_path)
        if not sticker_path.exists() or not sticker_path.is_file():
            _log(_LOG_WARN, "telegram", f"Sticker not found: {sticker_path}")
            return False
        return self._send_media_multipart(chat_id, sticker_path, "sticker", "sendSticker")

    def send_chat_action(self, chat_id: ChatId, action: str) -> None:
        """Send a chat action indicator (typing, uploading, etc.)."""
        telegram_api("sendChatAction", {"chat_id": chat_id, "action": action})

    def set_reaction(self, chat_id: ChatId, message_id: MessageId,
                     reaction: list[dict[str, str]]) -> None:
        """Set an emoji reaction on a message."""
        telegram_api("setMessageReaction", {"chat_id": chat_id, "message_id": message_id, "reaction": reaction})

    def edit_message(self, chat_id: ChatId, message_id: MessageId, text: str,
                     parse_mode: ParseMode = None) -> TelegramApiResponse:
        """Edit an existing message by its ID."""
        payload = {"chat_id": chat_id, "message_id": message_id, "text": text}
        if parse_mode:
            payload["parse_mode"] = parse_mode
        return telegram_api("editMessageText", payload)

    def setup_commands(self, commands: list[dict[str, str]]) -> None:
        """Register bot command suggestions with Telegram."""
        telegram_api("setMyCommands", {"commands": commands})

    def download_file(self, file_id: str, session_name: str) -> str | None:
        """Download a file from Telegram by file ID."""
        if not BOT_TOKEN:
            return None
        try:
            req = urllib.request.Request(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getFile",
                data=json.dumps({"file_id": file_id}).encode(),
                headers={"Content-Type": "application/json"}
            )
            with _urlopen(req, timeout=TIMEOUT_HTTP_DOWNLOAD) as r:
                result = cast(TelegramApiResponseDict, json.loads(r.read()))
                if not result.get("ok"):
                    _log(_LOG_WARN, "bridge", f"getFile failed: {result}")
                    return None
                _file_result = result.get("result", {})
                file_info = _file_result if isinstance(_file_result, dict) else {}
        except (urllib.error.URLError, OSError, TimeoutError) as e:
            _log(_LOG_ERROR, "bridge", f"getFile error: {e}")
            return None
        file_path = str(file_info.get("file_path", ""))
        file_size = int(file_info.get("file_size", 0))
        if not file_path:
            _log(_LOG_WARN, "telegram", "No file_path in response")
            return None
        if file_size > MAX_FILE_SIZE:
            _log(_LOG_WARN, "telegram", f"File too large: {file_size} > {MAX_FILE_SIZE}")
            return None
        download_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"
        import bridge as _br
        import claudecode as _cc
        inbox = _br.ensure_inbox_dir(session_name)
        ext = Path(file_path).suffix or ""
        local_filename = f"{uuid.uuid4().hex}{ext}"
        local_path = inbox / local_filename
        try:
            req = urllib.request.Request(download_url)
            with _urlopen(req, timeout=TIMEOUT_HTTP_UPLOAD) as r:
                content = r.read()
                if len(content) > MAX_FILE_SIZE:
                    _log(_LOG_WARN, "telegram", f"Downloaded file too large: {len(content)}")
                    return None
                local_path.write_bytes(content)
                local_path.chmod(0o600)
            _log(_LOG_INFO, "telegram", f"Downloaded file: {local_path}")
            host = _br.get_worker_host(session_name)
            if host:
                remote_inbox = str(inbox)
                _cc._remote_run(["mkdir", "-p", remote_inbox], host=host, capture_output=True, timeout=TIMEOUT_TMUX_CHECK)
                _cc._remote_run(["chmod", "700", remote_inbox], host=host, capture_output=True, timeout=TIMEOUT_TMUX_CHECK)
                rsync_result = _subprocess_runner.run(
                    ["rsync", "-az", str(local_path), f"{host}:{remote_inbox}/"],
                    capture_output=True, text=True, timeout=TIMEOUT_FILE_TRANSFER)
                if rsync_result.returncode != 0:
                    _log(_LOG_ERROR, "bridge", f"rsync inbound failed (exit {rsync_result.returncode}): {host}:{remote_inbox}/ -> {rsync_result.stderr.strip()}")
            return str(local_path)
        except (subprocess.SubprocessError, OSError) as e:
            _log(_LOG_ERROR, "bridge", f"Download error: {e}")
            return None



class LocalTransport(MessageTransport):
    """Transport that logs messages to stdout. For testing without Telegram."""

    def __init__(self) -> None:
        """Initialize local (in-process) transport with a message log."""
        self._log_file: str = os.environ.get("TRANSPORT_LOG", "")

    @property
    def name(self) -> str:
        """Return the transport name identifier."""
        return "local"

    def _log(self, method: str, chat_id: ChatId, **kwargs: object) -> None:
        """Log a transport method call with optional kwargs for debugging."""
        msg = f"{method} chat_id={chat_id}"
        for k, v in kwargs.items():
            if v is not None:
                msg += f" {k}={v}"
        _log(_LOG_DEBUG, "local-transport", msg)
        if self._log_file:
            with open(self._log_file, "a") as f:
                f.write(msg + "\n")

    def send_text(self, chat_id: ChatId, text: str,
                  parse_mode: ParseMode = None,
                  reply_to: MessageId | None = None) -> TelegramApiResponse:
        """Send a plain text message."""
        self._log("send_text", chat_id, text=text[:200], parse_mode=parse_mode)
        return {"ok": True, "result": {"message_id": 1}}

    def send_rich_text(self, chat_id: ChatId, markdown: str,
                       reply_to: MessageId | None = None) -> TelegramApiResponse:
        """Send a rich-formatted (markdown) text message."""
        self._log("send_rich_text", chat_id, text=markdown[:200])
        return {"ok": True, "result": {"message_id": 1}}

    def send_photo(self, chat_id: ChatId, photo_path: str | Path,
                   caption: str | None = None) -> bool:
        """Send a photo to a Telegram chat."""
        self._log("send_photo", chat_id, path=photo_path, caption=caption)
        return True

    def send_document(self, chat_id: ChatId, doc_path: str | Path,
                      caption: str | None = None) -> bool:
        """Send a document file to a Telegram chat."""
        self._log("send_document", chat_id, path=doc_path, caption=caption)
        return True

    def send_animation(self, chat_id: ChatId, animation_path: str | Path,
                       caption: str | None = None) -> bool:
        """Send an animation (GIF/MP4) to a Telegram chat."""
        self._log("send_animation", chat_id, path=animation_path, caption=caption)
        return True

    def send_video(self, chat_id: ChatId, video_path: str | Path,
                   caption: str | None = None) -> bool:
        """Send a video to a Telegram chat."""
        self._log("send_video", chat_id, path=video_path, caption=caption)
        return True

    def send_audio(self, chat_id: ChatId, audio_path: str | Path,
                   caption: str | None = None) -> bool:
        """Send an audio file to a Telegram chat."""
        self._log("send_audio", chat_id, path=audio_path, caption=caption)
        return True

    def send_voice(self, chat_id: ChatId, voice_path: str | Path,
                   caption: str | None = None) -> bool:
        """Send a voice message to a Telegram chat."""
        self._log("send_voice", chat_id, path=voice_path, caption=caption)
        return True

    def send_sticker(self, chat_id: ChatId, sticker_path: str | Path) -> bool:
        """Send a sticker to a Telegram chat."""
        self._log("send_sticker", chat_id, path=sticker_path)
        return True

    def send_chat_action(self, chat_id: ChatId, action: str) -> None:
        """Send a chat action indicator (typing, uploading, etc.)."""
        self._log("send_chat_action", chat_id, action=action)

    def set_reaction(self, chat_id: ChatId, message_id: MessageId,
                     reaction: list[dict[str, str]]) -> None:
        """Set an emoji reaction on a message."""
        self._log("set_reaction", chat_id, message_id=message_id)

    def edit_message(self, chat_id: ChatId, message_id: MessageId, text: str,
                     parse_mode: ParseMode = None) -> TelegramApiResponse:
        """Edit an existing message by its ID."""
        self._log("edit_message", chat_id, message_id=message_id, text=text[:200])
        return {"ok": True, "result": {"message_id": message_id}}

    def setup_commands(self, commands: list[dict[str, str]]) -> None:
        """Register bot command suggestions with Telegram."""
        self._log("setup_commands", 0, count=len(commands))

    def download_file(self, file_id: str, session_name: str) -> str | None:
        """Download a file from Telegram by file ID."""
        self._log("download_file", 0, file_id=file_id, session=session_name)
        return None



def _init_transport() -> MessageTransport:
    """Create the appropriate MessageTransport based on TRANSPORT_MODE."""
    if TRANSPORT_MODE == "local":
        return LocalTransport()
    return TelegramTransport(BOT_TOKEN)



transport = _init_transport()



def telegram_api(method: str, data: Mapping[str, object]) -> TelegramApiResponse:
    """Low-level Telegram API call. Tests can mock this to intercept all outbound calls."""
    if TRANSPORT_MODE == "local":
        _log(_LOG_INFO, "local-transport", f"telegram_api {method} {str(data)[:100]}")
        return {"ok": True, "result": {"message_id": 1}}  # type: ignore[arg-type]
    if isinstance(transport, TelegramTransport):
        return transport._api.api(method, data)
    return None



def send_telegram_message(chat_id: ChatId, text: str,
                          parse_mode: ParseMode = None) -> TelegramApiResponse:
    """Send a Telegram message, optionally with parse_mode (HTML or MarkdownV2)."""
    return transport.send_text(chat_id, text, parse_mode=parse_mode)



def download_telegram_file(file_id: str, session_name: str | None) -> str | None:
    """Download a Telegram file to the session inbox.
    Tests can patch bridge.download_telegram_file to intercept file downloads.
    Delegates to transport.download_file() internally.
    """
    if session_name is None:
        return None
    return transport.download_file(file_id, session_name)



# Backward-compat module-level media stubs.
# Tests patch these (e.g. patch.object(bridge, 'send_voice', ...)).
# Production code routes through transport.*; these stubs allow test mocking.
def send_voice(chat_id: ChatId, voice_path: str, caption: str | None = None) -> bool:
    """Send a voice message to a Telegram chat."""
    return transport.send_voice(chat_id, voice_path, caption)



def send_photo(chat_id: ChatId, photo_path: str, caption: str | None = None) -> bool:
    """Send a photo to a Telegram chat."""
    return transport.send_photo(chat_id, photo_path, caption)



def send_animation(chat_id: ChatId, animation_path: str, caption: str | None = None) -> bool:
    """Send an animation (GIF/MP4) to a Telegram chat."""
    return transport.send_animation(chat_id, animation_path, caption)



def send_document(chat_id: ChatId, doc_path: str, caption: str | None = None) -> bool:
    """Send a document file to a Telegram chat."""
    return transport.send_document(chat_id, doc_path, caption)



def send_video(chat_id: ChatId, video_path: str, caption: str | None = None) -> bool:
    """Send a video to a Telegram chat."""
    return transport.send_video(chat_id, video_path, caption)



def send_audio(chat_id: ChatId, audio_path: str, caption: str | None = None) -> bool:
    """Send an audio file to a Telegram chat."""
    return transport.send_audio(chat_id, audio_path, caption)



def send_sticker(chat_id: ChatId, sticker_path: str) -> bool:
    """Send a sticker to a Telegram chat."""
    return transport.send_sticker(chat_id, sticker_path)



# ============================================================
# MEDIA HANDLING
# ============================================================

# ─────────────────────────────────────────────────────────────────────────────
# Image Handling
# ─────────────────────────────────────────────────────────────────────────────

# Max file size: 50MB (Telegram Bot API limit for uploads)
MAX_FILE_SIZE = 50 * 1024 * 1024


# Allowed image extensions for outgoing (sendPhoto + sendAnimation + sendVideo)
ALLOWED_IMAGE_EXTENSIONS = {
    # Photos (sendPhoto)
    ".jpg", ".jpeg", ".png", ".webp", ".bmp",
    # Animations (sendAnimation) - autoplay, loop, silent
    ".gif", ".mp4",
}


# Allowed document extensions for outgoing (common code, docs, data files)
ALLOWED_DOC_EXTENSIONS = {
    # Docs
    ".md", ".txt", ".rst", ".pdf",
    # Data
    ".json", ".csv", ".yaml", ".yml", ".toml", ".ini", ".cfg", ".xml",
    ".log", ".sql", ".patch", ".diff",
    # Code
    ".py", ".js", ".ts", ".jsx", ".tsx",
    ".go", ".rs", ".java", ".kt", ".swift",
    ".rb", ".php", ".c", ".cpp", ".h", ".hpp",
    ".sh", ".html", ".css", ".scss",
    # Archives
    ".zip", ".tar", ".gz",
    # Audio (sendAudio — shows player UI)
    ".mp3", ".m4a", ".flac", ".aac", ".wav",
    # Voice (sendVoice — shows voice bubble)
    ".ogg", ".opus", ".oga",
    # Video (sendVideo — shows video player)
    ".mp4", ".mov", ".avi", ".mkv", ".webm",
    # Stickers (sendSticker)
    ".tgs",
}


# Blocked extensions (secrets, keys, certificates)
BLOCKED_DOC_EXTENSIONS = {
    ".pem", ".key", ".p12", ".pfx", ".crt", ".cer", ".der",
    ".jks", ".keystore", ".kdb", ".pgp", ".gpg", ".asc",
}


# Blocked filenames (case-insensitive)
BLOCKED_FILENAMES = {
    ".env", ".npmrc", ".pypirc", ".netrc", ".git-credentials",
    "id_rsa", "id_ed25519", "id_dsa", "credentials", "kubeconfig",
}



def format_file_size(size_bytes: int) -> str:
    """Format file size in human-readable form."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    else:
        return f"{size_bytes / (1024 * 1024):.1f} MB"



# download_telegram_file removed — use download_telegram_file() instead


def transcribe_voice(file_path: str, timeout: int | None = None) -> str | None:
    """Transcribe a voice file via STT endpoint. Returns text or None on failure.

    Fail-open: any error (timeout, bad response, unreachable) returns None
    so the caller can fall back to delivering the raw audio file.
    """
    if not STT_ENDPOINT:
        return None
    if timeout is None:
        timeout = STT_TIMEOUT

    try:
        file_path_obj = Path(file_path)
        if not file_path_obj.exists():
            return None

        boundary = uuid.uuid4().hex
        body_parts = []
        body_parts.append(f"--{boundary}".encode())
        content_type = mimetypes.guess_type(str(file_path_obj))[0] or "audio/ogg"
        body_parts.append(f'Content-Disposition: form-data; name="file"; filename="{file_path_obj.name}"'.encode())
        body_parts.append(f"Content-Type: {content_type}".encode())
        body_parts.append(b"")
        body_parts.append(file_path_obj.read_bytes())
        body_parts.append(f"--{boundary}--".encode())
        body_parts.append(b"")
        body = b"\r\n".join(body_parts)

        req = urllib.request.Request(
            STT_ENDPOINT,
            data=body,
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"}
        )
        with _urlopen(req, timeout=timeout) as r:
            result = cast(TelegramApiResponseDict, json.loads(r.read()))
            text = str(result.get("text", "")).strip()
            if text:
                duration = str(result.get("audio_duration_s", "?"))
                _log(_LOG_INFO, "stt", f"STT transcribed: {len(text)} chars from {duration}s audio")
                return text
            return None
    except (urllib.error.URLError, OSError, TimeoutError) as e:
        _log(_LOG_ERROR, "bridge", f"STT error (fail-open): {e}")
        return None



TELEGRAM_PHOTO_MAX_SUM = 10000  # width + height must not exceed this

TELEGRAM_PHOTO_MAX_DIM = 5000   # neither dimension may exceed this



def _prepare_photo_for_telegram(photo_path: str | Path) -> tuple[bytes, str]:
    """Auto-resize a photo if it exceeds Telegram's sendPhoto limits.

    Telegram returns 400 Bad Request when width+height > 10000 or either
    dimension > 5000.  This function scales down proportionally when needed
    and returns (bytes, filename).  If no resize is needed, returns the
    original file bytes unchanged.
    """
    photo_path = Path(photo_path)
    try:
        from PIL import Image
        with Image.open(photo_path) as img:
            w, h = img.size
            needs_resize = (
                w + h > TELEGRAM_PHOTO_MAX_SUM or
                w > TELEGRAM_PHOTO_MAX_DIM or
                h > TELEGRAM_PHOTO_MAX_DIM
            )
            if needs_resize:
                # Scale proportionally so both constraints are satisfied
                scale = min(
                    TELEGRAM_PHOTO_MAX_DIM / max(w, 1),
                    TELEGRAM_PHOTO_MAX_DIM / max(h, 1),
                    TELEGRAM_PHOTO_MAX_SUM / max(w + h, 1),
                )
                new_w = int(w * scale)
                new_h = int(h * scale)
                resized = img.resize((new_w, new_h), Image.Resampling.LANCZOS)
                import io
                buf = io.BytesIO()
                fmt = img.format or ("PNG" if photo_path.suffix.lower() == ".png" else "JPEG")
                if fmt == "PNG" and resized.mode not in ("RGBA", "P", "L", "LA"):
                    resized = resized.convert("RGBA")
                elif fmt == "JPEG" and resized.mode != "RGB":
                    resized = resized.convert("RGB")
                resized.save(buf, format=fmt)
                _log(_LOG_INFO, "telegram", f"Photo auto-resized: {w}x{h} -> {new_w}x{new_h} for Telegram")
                return buf.getvalue(), photo_path.name
        return photo_path.read_bytes(), photo_path.name
    except ImportError:
        return photo_path.read_bytes(), photo_path.name



def validate_photo_path(photo_path: str | Path) -> FileValidation:
    """Validate a photo path. Returns FileValidation(ok, Path_or_error_str)."""
    photo_path = Path(photo_path)

    if not photo_path.exists():
        return FileValidation(False, f"Photo not found: {photo_path}")

    if not photo_path.is_file():
        return FileValidation(False, f"Not a file: {photo_path}")

    # Check extension
    if photo_path.suffix.lower() not in ALLOWED_IMAGE_EXTENSIONS:
        return FileValidation(False, f"Invalid image extension: {photo_path.suffix}")

    # Check size
    file_size = photo_path.stat().st_size
    if file_size > MAX_FILE_SIZE:
        return FileValidation(False, f"Photo too large: {file_size} > {MAX_FILE_SIZE}")

    return FileValidation(True, photo_path)



def is_blocked_filename(filename: str) -> bool:
    """Check if filename matches blocked patterns (secrets, credentials, etc.)."""
    name_lower = filename.lower()
    # Check exact filename matches
    if name_lower in BLOCKED_FILENAMES:
        return True
    # Check .env.* pattern
    if name_lower.startswith(".env"):
        return True
    return False



def validate_document_path(doc_path: str | Path) -> FileValidation:
    """Validate a document path. Returns FileValidation(ok, Path_or_error_str)."""
    doc_path = Path(doc_path)

    # Security: validate path exists and is regular file
    if not doc_path.exists():
        return FileValidation(False, f"Document not found: {doc_path}")

    if not doc_path.is_file():
        return FileValidation(False, f"Not a file: {doc_path}")

    # Security: check for blocked extensions (sensitive)
    ext_lower = doc_path.suffix.lower()
    if ext_lower in BLOCKED_DOC_EXTENSIONS:
        return FileValidation(False, f"Blocked extension (sensitive): {doc_path.suffix}")

    # Security: check for blocked filenames
    if is_blocked_filename(doc_path.name):
        return FileValidation(False, f"Blocked filename (sensitive): {doc_path.name}")

    # Check size
    file_size = doc_path.stat().st_size
    if file_size > MAX_FILE_SIZE:
        return FileValidation(False, f"Document too large: {file_size} > {MAX_FILE_SIZE}")

    # Note: No path restriction - workers can send from anywhere
    # Security is enforced via extension allowlist and filename blocklist

    return FileValidation(True, doc_path)



# Media extensions routed to specialized Telegram API methods
VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".mkv", ".webm"}

AUDIO_EXTENSIONS = {".mp3", ".m4a", ".flac", ".aac", ".wav"}

VOICE_EXTENSIONS = {".ogg", ".opus", ".oga"}

STICKER_EXTENSIONS = {".tgs"}  # animated stickers; static .webp handled by sendPhoto



# ============================================================
# MESSAGE FORMATTING
# ============================================================

CODE_FENCE_RE = re.compile(r"```.*?```", re.DOTALL)

INLINE_CODE_RE = re.compile(r"`[^`\n]*`")



def _split_protected_segments(text: str, pattern: re.Pattern[str]) -> list[tuple]:
    """Split text into (segment, is_protected) based on regex matches."""
    segments = []
    last = 0
    for match in pattern.finditer(text):
        if match.start() > last:
            segments.append((text[last:match.start()], False))
        segments.append((match.group(0), True))
        last = match.end()
    if last < len(text):
        segments.append((text[last:], False))
    return segments



def _collapse_excess_newlines(text: str) -> str:
    """Collapse 3+ newlines to 2, but avoid touching code blocks and inline code."""
    output = []
    for segment, protected in _split_protected_segments(text, CODE_FENCE_RE):
        if protected:
            output.append(segment)
            continue
        for inline_segment, inline_protected in _split_protected_segments(segment, INLINE_CODE_RE):
            if inline_protected:
                output.append(inline_segment)
            else:
                output.append(re.sub(r"\n{3,}", "\n\n", inline_segment))
    return "".join(output)



def _parse_media_tags(text: str, tag_name: str, validate_func: Callable[[str | Path], FileValidation]) -> tuple[str, list[tuple[str | None, str]]]:
    """Parse media tags, skipping escaped tags and code spans.

    Returns (clean_text, [(path, caption), ...]).
    """
    pattern = re.compile(rf"(\\)?\[\[{tag_name}:([^\]|]+)(?:\|([^\]]*))?\]\]")
    items = []
    removed = 0

    def replace_tag(match: re.Match[str]) -> str:
        """Replace HTML tag names in a sanitizer context."""
        nonlocal removed
        if match.group(1):
            # Escaped tag, return without the escape slash.
            return match.group(0)[1:]
        path = match.group(2).strip()
        caption = (match.group(3) or "").strip()
        ok, _ = validate_func(path)
        if ok:
            items.append((path, caption))
            removed += 1
            return ""
        return match.group(0)

    output = []
    for segment, protected in _split_protected_segments(text, CODE_FENCE_RE):
        if protected:
            output.append(segment)
            continue
        for inline_segment, inline_protected in _split_protected_segments(segment, INLINE_CODE_RE):
            if inline_protected:
                output.append(inline_segment)
            else:
                output.append(pattern.sub(replace_tag, inline_segment))

    clean_text = "".join(output)
    if removed:
        clean_text = _collapse_excess_newlines(clean_text).strip()
    return clean_text, items



def parse_image_tags(text: str) -> tuple[str, list[tuple[str | None, str]]]:
    """Parse [[image:/path|caption]] tags from text.

    Returns (clean_text, [(path, caption), ...]).
    """
    return _parse_media_tags(text, "image", validate_photo_path)



def parse_file_tags(text: str) -> tuple[str, list[tuple[str | None, str]]]:
    """Parse [[file:/path|caption]] tags from text.

    Returns (clean_text, [(path, caption), ...]).
    """
    return _parse_media_tags(text, "file", validate_document_path)



def escape_html(text: str) -> str:
    """Escape HTML special characters for Telegram's HTML parse mode.

    Must escape &, <, > to prevent Telegram from interpreting them as HTML tags.
    """
    return text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')



class _TelegramHTMLSanitizer(HTMLParser):
    """Sanitize HTML to only allow Telegram-safe tags and attributes."""

    SAFE_TAGS: frozenset[str] = frozenset({
        "b", "strong", "i", "em", "u", "ins", "s", "strike", "del",
        "code", "pre", "a", "blockquote", "span", "tg-emoji", "tg-spoiler",
    })
    SAFE_ATTRS: dict[str, frozenset[str]] = {
        "a": frozenset({"href"}),
        "code": frozenset({"class"}),
        "blockquote": frozenset({"expandable"}),
        "span": frozenset({"class"}),
        "tg-emoji": frozenset({"emoji-id"}),
    }

    def __init__(self, rejected_open_tags: list[str]) -> None:
        """Initialize sanitizer with shared rejected-tag tracker."""
        super().__init__(convert_charrefs=False)
        self._out: list[str] = []
        self._rejected_open_tags = rejected_open_tags

    def _escape_attr(self, value: str) -> str:
        """Escape an HTML attribute value (entities + quotes)."""
        return escape_html(value).replace('"', "&quot;")

    def _attrs_are_safe(self, tag: str, attrs: list[tuple[str, str | None]]) -> bool:
        """Check if all attributes on a tag are in the allowed set."""
        allowed = self.SAFE_ATTRS.get(tag, frozenset())
        seen: set[str] = set()
        for name, value in attrs:
            if name in seen or name not in allowed:
                return False
            seen.add(name)
            if tag == "a" and name == "href":
                if value is None:
                    return False
            elif tag == "code" and name == "class":
                if value is None or not value.startswith("language-"):
                    return False
            elif tag == "blockquote" and name == "expandable":
                if value not in (None, "", "expandable"):
                    return False
            elif tag == "span" and name == "class":
                if value != "tg-spoiler":
                    return False
            elif tag == "tg-emoji" and name == "emoji-id":
                if value is None:
                    return False
        return True

    def _render_start_tag(self, tag: str, attrs: list[tuple[str, str | None]]) -> str:
        """Render a safe opening tag with escaped attributes."""
        if not attrs:
            return f"<{tag}>"
        rendered = []
        for name, value in attrs:
            if value is None:
                rendered.append(name)
            else:
                rendered.append(f'{name}="{self._escape_attr(value)}"')
        return f"<{tag} {' '.join(rendered)}>"

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        """Process an opening HTML tag during sanitization."""
        accepted = tag in self.SAFE_TAGS and self._attrs_are_safe(tag, attrs)
        if accepted:
            self._out.append(self._render_start_tag(tag, attrs))
        else:
            self._out.append(escape_html(self.get_starttag_text() or f"<{tag}>"))
            self._rejected_open_tags.append(tag)

    def handle_endtag(self, tag: str) -> None:
        """Process a closing HTML tag during sanitization."""
        rejected_match = False
        for idx in range(len(self._rejected_open_tags) - 1, -1, -1):
            if self._rejected_open_tags[idx] == tag:
                rejected_match = True
                del self._rejected_open_tags[idx]
                break
        if tag in self.SAFE_TAGS and not rejected_match:
            self._out.append(f"</{tag}>")
        else:
            self._out.append(escape_html(f"</{tag}>"))

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        """Process a self-closing HTML tag during sanitization."""
        accepted = tag in self.SAFE_TAGS and self._attrs_are_safe(tag, attrs)
        if accepted:
            start = self._render_start_tag(tag, attrs)
            self._out.append(f"{start[:-1]}/>")
        else:
            self._out.append(escape_html(self.get_starttag_text() or f"<{tag}/>"))

    def handle_data(self, data: str) -> None:
        """Process raw text content during sanitization."""
        self._out.append(escape_html(data))

    def handle_entityref(self, name: str) -> None:
        """Process a named HTML entity during sanitization."""
        self._out.append(f"&{name};")

    def handle_charref(self, name: str) -> None:
        """Process a numeric HTML character reference during sanitization."""
        self._out.append(f"&#{name};")

    def handle_comment(self, data: str) -> None:
        """Discard HTML comments during sanitization."""
        self._out.append(escape_html(f"<!--{data}-->"))

    def html(self) -> str:
        """Return the sanitized HTML output."""
        return "".join(self._out)



def _sanitize_telegram_html(raw: str, rejected_open_tags: list[str]) -> str:
    """Sanitize HTML to only allow Telegram-safe tags and attributes."""
    if not raw:
        return ""
    sanitizer = _TelegramHTMLSanitizer(rejected_open_tags)
    sanitizer.feed(raw)
    sanitizer.close()
    return sanitizer.html()



def _render_md_inline_plain(children: list[MarkdownToken]) -> str:
    """Render inline markdown-it token children to plain text (for <pre> content)."""
    out: list[str] = []
    for tok in children:
        if tok.type in ("text", "code_inline"):
            out.append(tok.content)
        elif tok.type in ("softbreak", "hardbreak"):
            out.append(" ")
        elif tok.type == "image":
            out.append(tok.content or "image")
        elif tok.type in ("strong_open", "strong_close", "em_open", "em_close",
                          "s_open", "s_close", "link_open", "link_close",
                          "html_inline"):
            pass  # intentional no-op: skip formatting tokens
        else:
            if tok.content:
                out.append(tok.content)
    return "".join(out)



def _render_md_inline_html(children: list[MarkdownToken], rejected_open_tags: list[str]) -> str:
    """Render inline markdown-it token children to Telegram HTML."""
    out: list[str] = []
    for tok in children:
        if tok.type == "text":
            out.append(escape_html(tok.content))
        elif tok.type == "code_inline":
            out.append(f"<code>{escape_html(tok.content)}</code>")
        elif tok.type == "strong_open":
            out.append("<b>")
        elif tok.type == "strong_close":
            out.append("</b>")
        elif tok.type == "em_open":
            out.append("<i>")
        elif tok.type == "em_close":
            out.append("</i>")
        elif tok.type == "s_open":
            out.append("<s>")
        elif tok.type == "s_close":
            out.append("</s>")
        elif tok.type == "link_open":
            href = escape_html((tok.attrs or {}).get("href", ""))
            out.append(f'<a href="{href}">')
        elif tok.type == "link_close":
            out.append("</a>")
        elif tok.type == "softbreak":
            out.append("\n")
        elif tok.type == "hardbreak":
            out.append("\n")
        elif tok.type == "image":
            alt = escape_html(tok.content or "image")
            src = escape_html((tok.attrs or {}).get("src", ""))
            out.append(f'[{alt}]({src})')
        elif tok.type == "html_inline":
            out.append(_sanitize_telegram_html(tok.content, rejected_open_tags))
        else:
            if tok.content:
                out.append(escape_html(tok.content))
    return "".join(out)



def _render_table_as_pre(headers: list[str], rows: list[list[str]]) -> str:
    """Render markdown table rows as a <pre>-aligned column block."""
    all_rows = [headers] + rows
    if not all_rows or not all_rows[0]:
        return ""
    num_cols = max(len(r) for r in all_rows)
    col_widths = [0] * num_cols
    for row in all_rows:
        for ci, cell in enumerate(row):
            if ci < num_cols:
                col_widths[ci] = max(col_widths[ci], len(cell))
    lines: list[str] = []
    for ri, row in enumerate(all_rows):
        cols = []
        for ci in range(num_cols):
            cell = row[ci] if ci < len(row) else ""
            cols.append(escape_html(cell.ljust(col_widths[ci])))
        lines.append("  ".join(cols).rstrip())
        if ri == 0:
            lines.append("\u2550" * (sum(col_widths) + 2 * (num_cols - 1)))
    return f"<pre>{chr(10).join(lines)}</pre>\n"



# Pattern for detecting tabular lines (2+ columns separated by 2+ spaces)
_MULTI_SPACE_RE = re.compile(r'\S  +\S.*\S  +\S')



def _wrap_plain_tables(text: str) -> str:
    """Find consecutive tabular lines outside <pre> and wrap in <pre>."""
    parts = re.split(r'(<pre>.*?</pre>)', text, flags=re.DOTALL)
    out: list[str] = []
    for part in parts:
        if part.startswith('<pre>'):
            out.append(part)
            continue
        lines = part.split('\n')
        i = 0
        while i < len(lines):
            if _MULTI_SPACE_RE.search(lines[i]):
                run = [lines[i]]
                j = i + 1
                while j < len(lines) and (_MULTI_SPACE_RE.search(lines[j]) or lines[j].strip() == ''):
                    run.append(lines[j])
                    j += 1
                tabular_count = sum(1 for line in run if _MULTI_SPACE_RE.search(line))
                if tabular_count >= 2:
                    while run and run[-1].strip() == '':
                        j -= 1
                        run.pop()
                    raw = []
                    for rl in run:
                        plain = re.sub(r'<[^>]+>', '', rl)
                        plain = plain.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
                        raw.append(plain)
                    content = escape_html('\n'.join(raw))
                    out.append(f'<pre>{content}</pre>')
                    i = j
                else:
                    out.append(lines[i])
                    i += 1
            else:
                out.append(lines[i])
                i += 1
    return '\n'.join(out) if out else text



def markdown_to_telegram_html(text: str) -> str:
    """Convert markdown to Telegram-compatible HTML using markdown-it-py.

    Handles: bold, italic, strikethrough, code, code blocks, links,
    blockquotes, headings (as bold), lists, tables (as pre), hr.
    Unrecognized tokens degrade to plain text.
    """
    from markdown_it import MarkdownIt

    md = MarkdownIt("commonmark").enable("strikethrough").enable("table")
    tokens = md.parse(text)

    result: list[str] = []
    list_depth = 0
    ordered_counter: list[int] = []
    in_table = False
    table_row: list[str] = []
    table_headers: list[str] = []
    table_rows: list[list[str]] = []
    in_thead = False
    rejected_open_tags: list[str] = []

    i = 0
    while i < len(tokens):
        tok = tokens[i]

        if tok.type == "paragraph_open":
            pass  # intentional no-op: skip token
        elif tok.type == "paragraph_close":
            if not in_table:
                result.append("\n")
        elif tok.type == "inline":  # type: ignore[arg-type]
            if in_table:
                table_row.append(_render_md_inline_plain(cast(list[MarkdownToken], tok.children or [])))  # type: ignore[arg-type]
            else:
                result.append(_render_md_inline_html(cast(list[MarkdownToken], tok.children or []), rejected_open_tags))

        # Headings -> bold
        elif tok.type == "heading_open":
            result.append("<b>")
        elif tok.type == "heading_close":
            result.append("</b>\n")

        # Code blocks
        elif tok.type == "fence":
            lang = tok.info.strip() if tok.info else ""
            code = escape_html(tok.content.rstrip("\n"))
            if lang:
                result.append(f'<pre><code class="language-{escape_html(lang)}">{code}</code></pre>\n')
            else:
                result.append(f"<pre>{code}</pre>\n")
        elif tok.type == "code_block":
            code = escape_html(tok.content.rstrip("\n"))
            result.append(f"<pre>{code}</pre>\n")

        # Blockquotes
        elif tok.type == "blockquote_open":
            result.append("<blockquote>")
        elif tok.type == "blockquote_close":
            if result and result[-1].endswith("\n"):
                result[-1] = result[-1][:-1]
            result.append("</blockquote>\n")

        # Bullet lists
        elif tok.type == "bullet_list_open":
            list_depth += 1
        elif tok.type == "bullet_list_close":
            list_depth -= 1
            if list_depth == 0:
                result.append("\n")

        # Ordered lists
        elif tok.type == "ordered_list_open":
            list_depth += 1
            ordered_counter.append(0)
        elif tok.type == "ordered_list_close":
            list_depth -= 1
            ordered_counter.pop()
            if list_depth == 0:
                result.append("\n")

        # List items
        elif tok.type == "list_item_open":
            indent = "  " * (list_depth - 1)
            if ordered_counter:
                ordered_counter[-1] += 1
                result.append(f"{indent}{ordered_counter[-1]}. ")
            else:
                result.append(f"{indent}\u2022 ")
        elif tok.type == "list_item_close":
            if result and not result[-1].endswith("\n"):
                result.append("\n")

        # Tables -> <pre> aligned columns
        elif tok.type == "table_open":
            in_table = True
            table_headers = []
            table_rows = []
        elif tok.type == "table_close":
            in_table = False
            pre_block = _render_table_as_pre(table_headers, table_rows)
            if pre_block:
                result.append(pre_block)
            table_headers = []
            table_rows = []
        elif tok.type == "thead_open":
            in_thead = True
        elif tok.type == "thead_close":
            in_thead = False
        elif tok.type in ("tbody_open", "tbody_close"):
            pass  # intentional no-op: skip token
        elif tok.type == "tr_open":
            table_row = []
        elif tok.type == "tr_close":
            if in_thead:
                table_headers = table_row[:]
            else:
                table_rows.append(table_row[:])
            table_row = []
        elif tok.type in ("th_open", "th_close", "td_open", "td_close"):
            pass  # intentional no-op: skip token

        # Horizontal rule
        elif tok.type == "hr":
            result.append("\u2014\u2014\u2014\u2014\n")

        # HTML blocks
        elif tok.type == "html_block":
            result.append(_sanitize_telegram_html(tok.content, rejected_open_tags))

        else:
            if tok.content:
                result.append(escape_html(tok.content))

        i += 1

    output = "".join(result).strip()
    while "\n\n\n" in output:
        output = output.replace("\n\n\n", "\n\n")

    return _wrap_plain_tables(output)



def _pipe_tables_to_html(text: str) -> str:
    """Convert GFM pipe tables to HTML <table> for sendRichMessage.

    Telegram's sendRichMessage markdown parser doesn't recognize GFM pipe
    table syntax — it collapses table rows into a single paragraph.
    This converts pipe tables to HTML tables which sendRichMessage renders
    natively via RichBlockTable.
    """
    import re

    def _parse_row(line: str) -> list[str] | None:
        """Parse a pipe-delimited table row into cells, or None if not a table row."""
        line = line.strip()
        if line.startswith('|'):
            line = line[1:]
        if line.endswith('|'):
            line = line[:-1]
        return [cell.strip() for cell in line.split('|')]

    def _esc(s: str) -> str:
        """Escape HTML special characters in table cell text."""
        return s.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

    def _cell_md(s: str) -> str:
        """Escape HTML then convert inline markdown in a table cell."""
        s = _esc(s)
        s = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', s)
        s = re.sub(r'__(.+?)__', r'<b>\1</b>', s)
        s = re.sub(r'(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)', r'<i>\1</i>', s)
        s = re.sub(r'~~(.+?)~~', r'<s>\1</s>', s)
        s = re.sub(r'`([^`]+)`', r'<code>\1</code>', s)
        s = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', r'<a href="\2">\1</a>', s)
        return s

    lines = text.split('\n')
    result = []
    i = 0
    in_code_fence = False
    while i < len(lines):
        # Track fenced code blocks — don't convert tables inside them
        if re.match(r'^\s*(`{3,}|~{3,})', lines[i]):
            in_code_fence = not in_code_fence
            result.append(lines[i])
            i += 1
            continue

        # Detect pipe table: header row, separator row, then data rows
        if (not in_code_fence
                and i + 1 < len(lines)
                and '|' in lines[i]
                and re.match(r'^\s*\|[\s:]*-+[\s:]*(\|[\s:]*-+[\s:]*)*\|?\s*$', lines[i + 1])):
            headers = _parse_row(lines[i])

            # Parse alignment from separator
            aligns = []
            for cell in (_parse_row(lines[i + 1]) or []):
                cell = cell.strip()
                if cell.startswith(':') and cell.endswith(':'):
                    aligns.append(' style="text-align:center"')
                elif cell.endswith(':'):
                    aligns.append(' style="text-align:right"')
                else:
                    aligns.append('')

            html = ['<table>']
            html.append('<tr>')
            for j, h in enumerate(headers or []):
                align = aligns[j] if j < len(aligns) else ''
                html.append(f'<th{align}>{_cell_md(h)}</th>')
            html.append('</tr>')

            i += 2
            while i < len(lines) and '|' in lines[i] and lines[i].strip().startswith('|'):
                cells = _parse_row(lines[i])
                html.append('<tr>')
                for j, cell in enumerate(cells or []):
                    align = aligns[j] if j < len(aligns) else ''
                    html.append(f'<td{align}>{_cell_md(cell)}</td>')
                html.append('</tr>')
                i += 1

            html.append('</table>')
            result.append('\n'.join(html))
        else:
            result.append(lines[i])
            i += 1

    return '\n'.join(result)



def format_response_text(session_name: str, text: str) -> str:
    """Format response with session prefix. No escaping - Claude Code handles safety."""
    # Strip redundant worker name prefix to avoid "lee:\nlee: message" double prefix
    stripped = text.lstrip()
    prefix = f"{session_name}:"
    if stripped.lower().startswith(prefix.lower()):
        text = stripped[len(prefix):].lstrip()
    return f"<b>{session_name}:</b>\n{text}"



# ─────────────────────────────────────────────────────────────────────────────
# Message Splitting (Telegram 4096 char limit)
# ─────────────────────────────────────────────────────────────────────────────

TELEGRAM_MAX_LENGTH = 4096

TELEGRAM_RICH_MAX_LENGTH = 32768



def split_message(text: str, max_len: int=TELEGRAM_MAX_LENGTH) -> list[str]:
    """Split HTML text into chunks that fit within Telegram's message limit.

    HTML-aware: tracks open tags and closes/reopens them at split boundaries.
    Splits on safe boundaries: blank lines → newlines → spaces → hard cut.
    Returns list of valid HTML text chunks.
    """
    import re
    if len(text) <= max_len:
        return [text]

    # Regex for Telegram-supported HTML tags
    TAG_RE = re.compile(r'<(/?)(\w+)([^>]*)>')
    TRACKED_TAGS = frozenset(('b', 'i', 's', 'u', 'code', 'pre', 'a',
                              'strong', 'em', 'del', 'ins', 'strike', 'blockquote'))

    def _closing_tags(stack: list[tuple[str, str]]) -> str:
        """Generate closing tags for all open tags (reverse order)."""
        return "".join(f"</{tag}>" for tag, _ in reversed(stack))

    def _opening_tags(stack: list[tuple[str, str]]) -> str:
        """Generate opening tags for all open tags (original order)."""
        return "".join(full for _, full in stack)

    def _scan_tags(text: str) -> list[tuple[str, str]]:
        """Return the tag stack state after scanning text."""
        stack: list[tuple[str, str]] = []
        for m in TAG_RE.finditer(text):
            is_close = m.group(1) == '/'
            tag_name = m.group(2).lower()
            if tag_name not in TRACKED_TAGS:
                continue
            if is_close:
                for j in range(len(stack) - 1, -1, -1):
                    if stack[j][0] == tag_name:
                        stack.pop(j)
                        break
            else:
                stack.append((tag_name, m.group(0)))
        return stack

    def _find_split(text: str, budget: int) -> int:
        """Find best split point within budget chars.

        Priority: blank line → newline → space → hard cut.
        Avoids splitting inside HTML tags. Always returns >= 1.
        """
        if budget <= 0:
            budget = 1
        search = text[:budget]

        # Don't split inside a tag — find last '>' before budget
        last_tag_start = search.rfind('<')
        last_tag_end = search.rfind('>')
        if last_tag_start > last_tag_end:
            search = text[:last_tag_start]
            budget = last_tag_start

        for sep in ('\n\n', '\n', ' '):
            pos = search.rfind(sep)
            if pos > budget // 3:
                return pos + 1

        return max(budget, 1)  # Guarantee forward progress

    chunks = []
    remaining = text
    carry_stack: list[tuple[str, str]] = []  # Tags open from previous chunk

    while remaining:
        prefix = _opening_tags(carry_stack)
        available = max_len - len(prefix)

        # Close carry tags in final chunk too
        if len(prefix) + len(remaining) + len(_closing_tags(carry_stack)) <= max_len:
            suffix = _closing_tags(_scan_tags(prefix + remaining))
            chunks.append(prefix + remaining + suffix)
            break

        # Find split point with iterative backoff to guarantee max_len
        budget = available - 100  # Initial conservative reserve
        if budget < 100:
            budget = 100

        for _attempt in range(5):
            split_at = _find_split(remaining, budget)
            chunk_text = remaining[:split_at].rstrip()
            full_chunk = prefix + chunk_text
            open_stack = _scan_tags(full_chunk)
            suffix = _closing_tags(open_stack)

            if len(full_chunk) + len(suffix) <= max_len:
                break
            # Shrink budget and retry
            overshoot = len(full_chunk) + len(suffix) - max_len
            budget = max(budget - overshoot - 20, 100)
        else:
            # Last resort: hard cut to fit
            hard_limit = max_len - len(prefix) - len(suffix) - 10
            if hard_limit < 1:
                hard_limit = 1
            chunk_text = remaining[:hard_limit].rstrip()
            full_chunk = prefix + chunk_text
            open_stack = _scan_tags(full_chunk)
            suffix = _closing_tags(open_stack)
            split_at = hard_limit

        chunks.append(full_chunk + suffix)
        carry_stack = open_stack
        remaining = remaining[split_at:].lstrip()

        # Safety: prevent infinite loop
        if split_at == 0:
            # Force progress by consuming at least 1 char
            remaining = remaining[1:]

    return chunks



def format_multipart_messages(session_name: str, chunks: list[str]) -> list[str]:
    """Format chunks with session prefix (all chunks get prefix, no part numbers).

    Single chunk: "<b>name:</b>\ntext"
    Multiple chunks: "<b>name:</b>\ntext" (same format, no 1/3, 2/3 etc)
    """
    return [format_response_text(session_name, chunk) for chunk in chunks]



def setup_bot_commands() -> None:
    """Initial bot commands setup."""
    update_bot_commands()



def update_bot_commands() -> None:
    """Update bot commands including dynamic worker shortcuts."""
    commands = list(BOT_COMMANDS)  # Copy static commands

    # Add worker shortcuts (e.g., /lee, /chen)
    import bridge as _br
    registered = _br.get_registered_sessions()
    for name in sorted(registered.keys()):
        commands.append({"command": name, "description": f"Message {name}"})

    transport.setup_commands(commands)
    worker_count = len(registered)
    _log(_LOG_INFO, "telegram", f"Bot commands updated ({len(BOT_COMMANDS)} + {worker_count} workers)")



def get_manager_chat_id(name: str) -> ChatId | None:
    """Resolve manager chat ID for worker notifications.

    Priority:
      1) ADMIN_CHAT_ID (if configured)
      2) Session chat_id file
    """
    if admin_chat_id is not None:
        return admin_chat_id

    import claudecode as _cc
    chat_id_file = _cc.get_chat_id_file(name)
    if not chat_id_file.exists():
        return None

    try:
        value = chat_id_file.read_text().strip()
        return int(value) if value else None
    except OSError as e:
        _log(_LOG_WARN, "bridge", f"Failed to read chat_id for {name}: {e}")
        return None

