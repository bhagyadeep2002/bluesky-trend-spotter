from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

POST_COLLECTION = "app.bsky.feed.post"
V2_COMMIT = "network.bsky.jetstream.subscribeEvents#commit"


class MalformedEvent(ValueError):
    """A payload does not satisfy the supported event shape."""


@dataclass(frozen=True)
class PostEvent:
    event_key: str
    uri: str
    author_did: str
    rkey: str
    operation: str
    source_time: str | None
    cursor: int | None
    text: str | None
    normalized_text: str | None
    created_at: str | None
    languages: list[str]

    def to_dict(self) -> dict:
        return asdict(self)


def timestamp(value: object) -> str:
    if not isinstance(value, str):
        raise MalformedEvent("invalid timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError
        return parsed.astimezone(UTC).isoformat()
    except ValueError as exc:
        raise MalformedEvent("invalid timestamp") from exc


def reject_constant(value: str) -> None:
    raise MalformedEvent("nonstandard JSON constant")


def parse_event(raw: str | bytes) -> PostEvent | None:
    """Return None for unsupported event kinds/collections/operations."""
    try:
        event = json.loads(raw, parse_constant=reject_constant)
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise MalformedEvent("invalid JSON") from exc
    if not isinstance(event, dict):
        raise MalformedEvent("event must be an object")
    v2 = "$type" in event
    if v2:
        if event["$type"] != "message":
            return None
        body = event.get("payload")
        if not isinstance(body, dict):
            raise MalformedEvent("missing payload")
        if body.get("$type") != V2_COMMIT:
            return None
        commit = body
    else:
        if not isinstance(event.get("kind"), str):
            raise MalformedEvent("missing kind")
        if event["kind"] != "commit":
            return None
        body = event
        commit = body.get("commit")
    if not isinstance(commit, dict):
        raise MalformedEvent("missing commit")
    if not isinstance(commit.get("collection"), str) or not isinstance(
        commit.get("operation"), str
    ):
        raise MalformedEvent("missing collection or operation")
    if commit["collection"] != POST_COLLECTION or commit["operation"] not in {
        "create",
        "update",
        "delete",
    }:
        return None
    did, rkey = body.get("did"), commit.get("rkey")
    if not isinstance(did, str) or not re.fullmatch(r"did:[a-z0-9]+:[A-Za-z0-9._:%-]+", did):
        raise MalformedEvent("invalid repository DID")
    if (
        not isinstance(rkey, str)
        or not re.fullmatch(r"[A-Za-z0-9._~:-]{1,512}", rkey)
        or rkey in {".", ".."}
    ):
        raise MalformedEvent("invalid record key")
    cursor = body.get("seq" if v2 else "time_us")
    if cursor is not None and (type(cursor) is not int or cursor < 0):
        raise MalformedEvent("invalid cursor")
    source_time = None
    if v2 and body.get("time") is not None:
        source_time = timestamp(body["time"])
    elif not v2 and cursor is not None:
        try:
            source_time = datetime.fromtimestamp(cursor / 1_000_000, UTC).isoformat()
        except (ValueError, OverflowError, OSError) as exc:
            raise MalformedEvent("invalid source timestamp") from exc
    text, created_at, languages = None, None, []
    if commit["operation"] != "delete":
        record = commit.get("record")
        if not isinstance(record, dict) or not isinstance(record.get("text"), str):
            raise MalformedEvent("missing post text")
        if record.get("$type", POST_COLLECTION) != POST_COLLECTION:
            raise MalformedEvent("unexpected record type")
        text = record["text"]
        created_at = timestamp(record["createdAt"]) if "createdAt" in record else None
        languages = record.get("langs", [])
        if not isinstance(languages, list) or any(not isinstance(lang, str) for lang in languages):
            raise MalformedEvent("invalid languages")
    # Canonical source event digest: stable on duplicate delivery, no retained text in key.
    digest = hashlib.sha256(
        json.dumps(event, sort_keys=True, ensure_ascii=True, separators=(",", ":")).encode()
    ).hexdigest()
    return PostEvent(
        digest,
        f"at://{did}/{POST_COLLECTION}/{rkey}",
        did,
        rkey,
        commit["operation"],
        source_time,
        cursor,
        text,
        " ".join(unicodedata.normalize("NFC", text).split()) if text is not None else None,
        created_at,
        languages,
    )
