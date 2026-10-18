"""Atomic cursor acknowledgements ordered after raw and downstream persistence."""

import hashlib
import json
import os
from pathlib import Path


class Checkpoint:
    def __init__(self, directory: Path, source: str):
        self.path = directory / "checkpoint.json"
        self.source = hashlib.sha256(source.encode()).hexdigest()

    def load(self) -> int | None:
        if not self.path.exists():
            return None
        value = json.loads(self.path.read_text())
        if value.get("source") != self.source:
            raise ValueError("checkpoint source differs; use a separate raw directory")
        cursor = value.get("cursor")
        if type(cursor) is not int or cursor < 0:
            raise ValueError("invalid checkpoint cursor")
        return cursor

    def save(self, cursor: int) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(".tmp")
        with temporary.open("w") as stream:
            json.dump({"version": 1, "source": self.source, "cursor": cursor}, stream)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, self.path)
        descriptor = os.open(self.path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
