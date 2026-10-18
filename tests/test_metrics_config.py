import unittest
import json
import tempfile
from pathlib import Path
from unittest.mock import patch
from bluesky_ingest.config import Settings
from bluesky_ingest.metrics import Metrics, create_app

class SyncTests(unittest.TestCase):

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.tmp_path = Path(self.temp.name)
        self.messages = (Path(__file__).parent / 'fixtures/events.jsonl').read_text().splitlines()

    def test_invalid_settings(self):
        for values in [{'queue_size': 0}, {'raw_rotate_seconds': -1}, {'backoff_max': 0.5}, {'backoff_initial': float('nan')}, {'metrics_port': 65536}, {'jetstream_url': 'https://invalid'}, {'jetstream_url': 'ws://user:secret@example.com'}]:
            with self.subTest(values=values):
                with self.assertRaises(ValueError):
                    Settings(**values)

    def test_environment(self):
        tmp_path = self.tmp_path
        import os
        with patch.dict(os.environ, {'RAW_DIR': str(tmp_path), 'QUEUE_SIZE': '3'}):
            settings = Settings.from_env()
        assert settings.raw_dir == tmp_path
        assert settings.queue_size == 3

class AsyncTests(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.tmp_path = Path(self.temp.name)
        self.messages = (Path(__file__).parent / 'fixtures/events.jsonl').read_text().splitlines()

    async def test_http_metrics(self):
        metrics = Metrics(events_received=10, events_persisted=8, connection_state='connected')
        app = create_app(metrics)

        async def request(path):
            sent = []

            async def receive():
                return {'type': 'http.request', 'body': b'', 'more_body': False}

            async def send(message):
                sent.append(message)
            await app({'type': 'http', 'asgi': {'version': '3.0'}, 'method': 'GET', 'scheme': 'http', 'path': path, 'query_string': b'', 'headers': [], 'server': ('local', 80), 'client': ('local', 1)}, receive, send)
            status = next((m['status'] for m in sent if m['type'] == 'http.response.start'))
            body = b''.join((m.get('body', b'') for m in sent if m['type'] == 'http.response.body'))
            return (status, json.loads(body))
        status, body = await request('/healthz')
        assert status == 200 and body == {'status': 'ok'}
        status, body = await request('/api/v1/pipeline/status')
        assert status == 200
        assert body['events_received'] == 10
        assert body['persisted_per_second'] > 0
        metrics.connection_state = 'failed'
        assert (await request('/healthz'))[0] == 503
