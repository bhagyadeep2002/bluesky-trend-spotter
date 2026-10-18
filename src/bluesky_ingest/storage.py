from __future__ import annotations

import base64
import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import BinaryIO, Callable
from uuid import uuid4


class RawWriter:
    """Single-writer append log. Acknowledges only after flush + fsync."""

    def __init__(
        self,
        directory: Path,
        max_bytes: int,
        rotate_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.directory = directory
        self.max_bytes = max_bytes
        self.rotate_seconds = rotate_seconds
        self.clock = clock
        self.file: BinaryIO | None = None
        self.size = 0
        self.opened_at = 0.0

    def append(self, envelope: dict) -> str:
        line = (json.dumps(envelope, ensure_ascii=True, separators=(",", ":")) + "\n").encode()
        now = self.clock()
        if (
            self.file is None
            or self.size + len(line) > self.max_bytes
            or now - self.opened_at >= self.rotate_seconds
        ):
            self.close()
            self.directory.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
            self.file = (self.directory / f"events-{stamp}-{uuid4().hex}.jsonl").open("xb")
            self.size, self.opened_at = 0, now
            # Persist the new file's directory entry as well as its contents.
            fd = os.open(self.directory, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        reference = f"{Path(self.file.name).name}:{self.size}"
        self.file.write(line)
        self.file.flush()
        os.fsync(self.file.fileno())
        self.size += len(line)
        return reference

    def close(self) -> None:
        if self.file is not None:
            self.file.close()
            self.file = None


def raw_fields(raw: str | bytes) -> dict:
    if isinstance(raw, bytes):
        return {"raw_encoding": "base64", "raw": base64.b64encode(raw).decode("ascii")}
    return {"raw_encoding": "text", "raw": raw}


def replay_messages(path: Path, max_bytes: int):
    """Stream one file, either our envelopes or plain source JSONL fixtures."""
    with path.open("rb") as stream:
        # JSON escaping/base64 and normalized fields can expand a source message.
        while line := stream.readline(max_bytes * 12 + 1):
            if len(line) > max_bytes * 12:
                raise ValueError("replay line exceeds configured size limit")
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except ValueError, UnicodeError, RecursionError:
                yield line.rstrip(b"\r\n")
                continue
            if isinstance(value, dict) and value.get("schema_version") == 1:
                raw = value["raw"]
                yield (
                    base64.b64decode(raw, validate=True)
                    if value["raw_encoding"] == "base64"
                    else raw
                )
            else:
                yield line.rstrip(b"\r\n")
