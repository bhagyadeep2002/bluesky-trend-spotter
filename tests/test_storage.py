import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bluesky_ingest.storage import RawWriter, raw_fields, replay_messages


class SyncTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.tmp_path = Path(self.temp.name)
        self.messages = (Path(__file__).parent / "fixtures/events.jsonl").read_text().splitlines()

    def test_size_rotation_and_restart(self):
        tmp_path = self.tmp_path
        for _ in range(2):
            writer = RawWriter(tmp_path, max_bytes=15, rotate_seconds=300)
            writer.append({"x": "one"})
            writer.append({"x": "two"})
            writer.close()
        files = sorted(tmp_path.glob("*.jsonl"))
        assert len(files) == 4
        assert [json.loads(p.read_text())["x"] for p in files] == ["one", "two", "one", "two"]

    def test_time_rotation(self):
        tmp_path = self.tmp_path
        ticks = iter([0.0, 4.0, 5.0])
        writer = RawWriter(tmp_path, 1000, 5, clock=lambda: next(ticks))
        for i in range(3):
            writer.append({"i": i})
        writer.close()
        files = sorted(tmp_path.glob("*.jsonl"))
        assert [len(p.read_text().splitlines()) for p in files] == [2, 1]

    def test_oversized_envelope_stays_one_line(self):
        tmp_path = self.tmp_path
        writer = RawWriter(tmp_path, 2, 300)
        writer.append({"text": "escaped\nnewline"})
        writer.close()
        assert len(next(tmp_path.glob("*.jsonl")).read_text().splitlines()) == 1

    def test_lossless_raw_replay(self):
        for raw in ['{"hello":"world"}', b"\xff\x00", "broken json"]:
            tmp_path = self.tmp_path / str(type(raw).__name__) / str(len(raw))
            with self.subTest(raw=raw):
                writer = RawWriter(tmp_path, 10000, 300)
                writer.append({"schema_version": 1, **raw_fields(raw)})
                writer.close()
                assert list(replay_messages(next(tmp_path.glob("*.jsonl")), 10000)) == [raw]

    def test_fsync_failure_is_not_acknowledged(self):
        tmp_path = self.tmp_path
        writer = RawWriter(tmp_path, 1000, 300)
        writer.append({"ok": True})

        def fail(fd):
            raise OSError("disk unavailable")

        with patch("bluesky_ingest.storage.os.fsync", fail):
            with self.assertRaises(OSError):
                writer.append({"ok": False})
        writer.close()
