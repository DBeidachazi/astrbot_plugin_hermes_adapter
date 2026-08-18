"""Wire helpers shared by the AstrBot connector and its tests."""

from __future__ import annotations

import hashlib
from pathlib import Path, PurePosixPath
from urllib.parse import quote, urlsplit, urlunsplit


PROTOCOL_VERSION = 1
CHUNK_SIZE = 256 * 1024


def build_websocket_url(gateway_url: str, profile: str) -> str:
    """Build the profile-scoped AstrBot adapter WebSocket URL."""
    raw = gateway_url.strip().rstrip("/")
    parsed = urlsplit(raw)
    if parsed.scheme not in {"http", "https", "ws", "wss"} or not parsed.netloc:
        raise ValueError("Hermes adapter URL must be an http(s) or ws(s) URL")

    scheme = {"http": "ws", "https": "wss"}.get(parsed.scheme, parsed.scheme)
    path = parsed.path.rstrip("/")
    segments = [segment for segment in path.split("/") if segment]
    if len(segments) >= 2 and segments[-2] == "p":
        path = f"{path}/astrbot/ws"
    else:
        path = f"{path}/p/{quote(profile.strip() or 'default', safe='')}/astrbot/ws"
    return urlunsplit((scheme, parsed.netloc, path, parsed.query, ""))


def build_chat_id(platform: str, user_id: str, group_id: str = "") -> str:
    """Use one shared conversation per group and one per private user."""
    safe_platform = _identity_part(platform)
    if group_id:
        return f"astrbot:{safe_platform}:group:{_identity_part(group_id)}"
    return f"astrbot:{safe_platform}:private:{_identity_part(user_id)}"


def safe_filename(value: str | None, fallback: str = "file") -> str:
    normalized = str(value or "").replace("\\", "/").replace("\x00", "")
    name = PurePosixPath(normalized).name.strip()
    if not name or name in {".", ".."}:
        return fallback
    return "".join("_" if ord(ch) < 32 or ch in ':*?\"<>|' else ch for ch in name)


def sha256_path(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while chunk := handle.read(CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()


def resolve_delivery_temp_dir() -> Path:
    """Resolve a directory for receiving incoming media attachments.

    Prefer `/AstrBot/data/temp` (shared Docker volume with NapCat) so
    NapCat can read the file directly when uploading to QQ.
    """
    import tempfile
    import uuid

    candidates = [
        Path("/AstrBot/data/temp"),
        Path("/AstrBot/data/cache"),
        Path(tempfile.gettempdir()),
    ]
    for candidate in candidates:
        try:
            candidate.mkdir(parents=True, exist_ok=True)
            test_file = candidate / f".probe_{uuid.uuid4().hex}"
            test_file.touch()
            test_file.unlink()
            return candidate
        except Exception:
            continue
    return Path(tempfile.gettempdir())


def _identity_part(value: str) -> str:
    text = str(value or "unknown").strip()
    return text.replace(":", "_").replace("/", "_") or "unknown"

