import unittest
import json
import tempfile
from pathlib import Path
from unittest.mock import patch
from bluesky_ingest.events import MalformedEvent, parse_event

class SyncTests(unittest.TestCase):

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.tmp_path = Path(self.temp.name)
        self.messages = (Path(__file__).parent / 'fixtures/events.jsonl').read_text().splitlines()

    def test_operations_and_normalization(self):
        messages = self.messages
        create, update, delete = [parse_event(raw) for raw in messages[:3]]
        assert create.operation == 'create'
        assert create.text == 'Hello  #Bluesky\nworld'
        assert create.normalized_text == 'Hello #Bluesky world'
        assert create.languages == ['en']
        assert create.uri == update.uri == delete.uri
        assert update.text == 'Updated #Bluesky'
        assert delete.text is None
        assert delete.normalized_text is None
        assert len({item.event_key for item in (create, update, delete)}) == 3

    def test_v2(self):
        messages = self.messages
        post = parse_event(messages[5])
        assert post.cursor == 123456
        assert post.source_time == '2026-10-18T12:00:00+00:00'
        assert post.rkey == 'post2'

    def test_duplicate_keys_stable(self):
        create_event = json.loads(self.messages[0])
        assert parse_event(json.dumps(create_event)).event_key == parse_event(json.dumps(create_event, sort_keys=True, indent=2)).event_key

    def test_malformed(self):
        for raw in ['{', '[]', 'null', '{}', '{"kind":"commit"}', b'\xff', '{"kind":NaN}', '{"$type":"message","payload":null}']:
            with self.subTest(raw=raw):
                with self.assertRaises(MalformedEvent):
                    parse_event(raw)

    def test_bad_metadata(self):
        for field, value in [('did', None), ('did', 'invalid'), ('time_us', True), ('time_us', -1), ('time_us', 10 ** 100)]:
            create_event = json.loads(self.messages[0])
            with self.subTest(field=field, value=value):
                create_event[field] = value
                with self.assertRaises(MalformedEvent):
                    parse_event(json.dumps(create_event))

    def test_bad_record(self):
        for field, value in [('text', 42), ('langs', 'en'), ('langs', [None]), ('createdAt', 'yesterday'), ('createdAt', '2026-10-18T12:00:00')]:
            create_event = json.loads(self.messages[0])
            with self.subTest(field=field, value=value):
                create_event['commit']['record'][field] = value
                with self.assertRaises(MalformedEvent):
                    parse_event(json.dumps(create_event))

    def test_unknown_kind(self):
        for raw in ['{"kind":"future_kind"}', '{"$type":"message","payload":{"$type":"future"}}']:
            with self.subTest(raw=raw):
                assert parse_event(raw) is None

    def test_other_collection_and_future_operation(self):
        create_event = json.loads(self.messages[0])
        create_event['commit']['operation'] = 'future'
        assert parse_event(json.dumps(create_event)) is None
        create_event['commit']['collection'] = 'app.bsky.feed.like'
        assert parse_event(json.dumps(create_event)) is None

    def test_missing_source_time(self):
        create_event = json.loads(self.messages[0])
        del create_event['time_us']
        post = parse_event(json.dumps(create_event))
        assert post.cursor is None and post.source_time is None
