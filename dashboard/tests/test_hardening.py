import unittest
import tempfile
import os
import json
import time
import threading
import urllib.request
import urllib.error
import urllib.parse
import http.client
import socket
from pathlib import Path


class TestHardening(unittest.TestCase):
    """Security hardening and edge-case tests for the dashboard server."""

    @classmethod
    def setUpClass(cls):
        # Create temporary SWARM_DIR with test data
        cls.tmpdir = tempfile.mkdtemp(prefix="swarm_hardening_")
        cls.swarm_dir = Path(cls.tmpdir)
        cls._create_test_run()
        
        # Start server without token
        from dashboard.server import make_server
        cls.server = make_server("127.0.0.1", 0, cls.tmpdir, token=None)
        cls.port = cls.server.server_address[1]
        cls.base_url = f"http://127.0.0.1:{cls.port}"
        
        cls.server_thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.server_thread.start()
        
        # Wait for server to be ready
        cls._wait_for_server()
        
        # Start second server with token for auth tests
        cls.tmpdir2 = tempfile.mkdtemp(prefix="swarm_hardening_auth_")
        cls._create_minimal_run(cls.tmpdir2)
        cls.server_auth = make_server("127.0.0.1", 0, cls.tmpdir2, token="test-token")
        cls.port_auth = cls.server_auth.server_address[1]
        cls.base_url_auth = f"http://127.0.0.1:{cls.port_auth}"
        cls.server_auth_thread = threading.Thread(target=cls.server_auth.serve_forever, daemon=True)
        cls.server_auth_thread.start()
        cls._wait_for_server(cls.base_url_auth)

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.server_auth.shutdown()
        cls.server_auth.server_close()

    @classmethod
    def _wait_for_server(cls, base_url=None):
        url = base_url or cls.base_url
        for _ in range(50):
            try:
                urllib.request.urlopen(f"{url}/api/runs", timeout=0.5)
                return
            except urllib.error.HTTPError:
                # Any HTTP response (including 401) means server is up
                return
            except (urllib.error.URLError, ConnectionRefusedError, socket.timeout):
                time.sleep(0.05)
        raise RuntimeError("Server did not start in time")

    @classmethod
    def _create_test_run(cls):
        """Create a test run with events for SSE testing."""
        run_dir = cls.swarm_dir / "20261121-125023"
        run_dir.mkdir(parents=True)
        task_dir = run_dir / "task1"
        task_dir.mkdir()
        
        tasks = {
            "name": "Hardening Test Run",
            "tasks": [
                {"id": "task1", "title": "Test Task", "instructions": "Test"}
            ]
        }
        (run_dir / "tasks.json").write_text(json.dumps(tasks))
        
        # Initial events for SSE testing
        events = [
            {"ts": 1700000000.0, "run": "20261121-125023", "task": None, "type": "run_start", "name": "Hardening Test Run", "tasks": [{"id": "task1", "title": "Test Task"}]},
            {"ts": 1700000001.0, "run": "20261121-125023", "task": "task1", "type": "task_queued", "title": "Test Task"},
            {"ts": 1700000002.0, "run": "20261121-125023", "task": "task1", "type": "task_start", "title": "Test Task"},
        ]
        with open(run_dir / "events.jsonl", "w") as f:
            for ev in events:
                f.write(json.dumps(ev) + "\n")

    @classmethod
    def _create_minimal_run(cls, tmpdir):
        """Create a minimal run for auth testing."""
        run_dir = Path(tmpdir) / "20261121-140000"
        run_dir.mkdir(parents=True)
        (run_dir / "tasks.json").write_text(json.dumps({"name": "Auth Test", "tasks": []}))
        (run_dir / "events.jsonl").write_text('{"ts": 1700002000.0, "run": "20261121-140000", "task": null, "type": "run_start", "name": "Auth Test", "tasks": []}\n')

    def _get(self, path, token=None, base_url=None, timeout=5):
        """Make a GET request and return (status, headers, body)."""
        url = (base_url or self.base_url) + path
        req = urllib.request.Request(url)
        if token:
            req.add_header("Authorization", f"Bearer {token}")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.status, resp.headers, resp.read().decode("utf-8")
        except urllib.error.HTTPError as e:
            return e.status, e.headers, e.read().decode("utf-8")
        except urllib.error.URLError as e:
            raise RuntimeError(f"Request failed: {e}")

    def _get_json(self, path, token=None, base_url=None):
        status, headers, body = self._get(path, token, base_url)
        self.assertEqual(status, 200, f"Expected 200, got {status}: {body}")
        return json.loads(body)

    def _http_get_raw(self, path, host="127.0.0.1", port=None, headers=None, timeout=5):
        """Make a raw HTTP request using http.client (no path normalization)."""
        port = port or self.port
        conn = http.client.HTTPConnection(host, port, timeout=timeout)
        conn.request("GET", path, headers=headers or {})
        resp = conn.getresponse()
        body = resp.read().decode("utf-8")
        conn.close()
        return resp.status, resp.headers, body

    # ===== /healthz tests =====
    
    def test_healthz_unauthenticated_on_token_server(self):
        """GET /healthz returns 200 {ok:true} even on token-protected server without token."""
        status, headers, body = self._get("/healthz", base_url=self.base_url_auth)
        self.assertEqual(status, 200)
        data = json.loads(body)
        self.assertEqual(data, {"ok": True})
        
        # Also test on non-token server
        status, headers, body = self._get("/healthz", base_url=self.base_url)
        self.assertEqual(status, 200)
        data = json.loads(body)
        self.assertEqual(data, {"ok": True})

    # ===== Path traversal / reserved names tests =====
    
    def test_run_id_dot_returns_404(self):
        """Run ID '.' returns 404 for /api/runs/<id> (using raw http.client)."""
        # Use http.client to send verbatim path without normalization
        status, headers, body = self._http_get_raw("/api/runs/.")
        self.assertEqual(status, 404)
        data = json.loads(body)
        self.assertIn("error", data)
    
    def test_run_id_dotdot_returns_404(self):
        """Run ID '..' returns 404 for /api/runs/<id> (using raw http.client)."""
        status, headers, body = self._http_get_raw("/api/runs/..")
        self.assertEqual(status, 404)
        data = json.loads(body)
        self.assertIn("error", data)
    
    def test_task_live_dot_returns_404(self):
        """Task ID '.' returns 404 for /tasks/<id>/live (using raw http.client)."""
        status, headers, body = self._http_get_raw("/api/runs/20261121-125023/tasks/./live")
        self.assertEqual(status, 404)
    
    def test_task_live_dotdot_returns_404(self):
        """Task ID '..' returns 404 for /tasks/<id>/live (using raw http.client)."""
        status, headers, body = self._http_get_raw("/api/runs/20261121-125023/tasks/../live")
        self.assertEqual(status, 404)

    # ===== Security headers tests =====
    
    def test_security_headers_on_json_response(self):
        """Security headers present on a JSON response (/api/runs)."""
        status, headers, body = self._get("/api/runs")
        self.assertEqual(status, 200)
        
        self.assertEqual(headers.get("X-Content-Type-Options"), "nosniff")
        self.assertEqual(headers.get("Referrer-Policy"), "no-referrer")
        self.assertEqual(headers.get("X-Frame-Options"), "DENY")
        csp = headers.get("Content-Security-Policy", "")
        self.assertIn("default-src 'self'", csp)
        self.assertIn("style-src 'self' 'unsafe-inline'", csp)
        self.assertIn("img-src 'self' data:", csp)
        self.assertIn("connect-src 'self'", csp)
        self.assertIn("frame-ancestors 'none'", csp)
    
    def test_security_headers_on_root(self):
        """Security headers present on '/' (static index)."""
        status, headers, body = self._get("/")
        self.assertEqual(status, 200)
        
        self.assertEqual(headers.get("X-Content-Type-Options"), "nosniff")
        self.assertEqual(headers.get("Referrer-Policy"), "no-referrer")
        self.assertEqual(headers.get("X-Frame-Options"), "DENY")
        csp = headers.get("Content-Security-Policy", "")
        self.assertIn("default-src 'self'", csp)
        self.assertIn("style-src 'self' 'unsafe-inline'", csp)
        self.assertIn("img-src 'self' data:", csp)
        self.assertIn("connect-src 'self'", csp)
        self.assertIn("frame-ancestors 'none'", csp)

    # ===== Auth tests =====
    
    def test_wrong_token_same_length_returns_401(self):
        """Wrong token of same length returns 401 (timing-safe compare)."""
        # Token is "test-token" (10 chars), use "wrong-token" (11 chars) or "bad-tokenxx" (10 chars)
        # Actually "test-token" is 10 chars, let's use "xxxxxxxxxx" (10 chars)
        status, _, body = self._get("/api/runs?token=xxxxxxxxxx", base_url=self.base_url_auth)
        self.assertEqual(status, 401)
        data = json.loads(body)
        self.assertIn("error", data)
        
        # Also test via header
        status, _, body = self._get("/api/runs", token="xxxxxxxxxx", base_url=self.base_url_auth)
        self.assertEqual(status, 401)

    # ===== SSE tests =====
    
    def test_sse_event_lines_include_id_field(self):
        """SSE event lines include an 'id:' line before 'data:'."""
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        conn.request("GET", "/api/events", headers={"Accept": "text/event-stream"})
        resp = conn.getresponse()
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.getheader("Content-Type"), "text/event-stream; charset=utf-8")
        
        # Wait a bit for initial connection
        time.sleep(0.2)
        
        # Append a new event to trigger SSE
        run_events = self.swarm_dir / "20261121-125023" / "events.jsonl"
        new_event = {"ts": time.time(), "run": "20261121-125023", "task": "task1", "type": "progress", "chars": 100, "model": "model-x"}
        with open(run_events, "a") as f:
            f.write(json.dumps(new_event) + "\n")
        
        # Read from SSE stream and check for id: field
        deadline = time.time() + 5
        found_id_field = False
        found_data_field = False
        id_before_data = False
        last_was_id = False
        
        while time.time() < deadline:
            line = resp.readline()
            if not line:
                break
            line = line.decode("utf-8").rstrip("\n")
            if line.startswith("id: "):
                found_id_field = True
                last_was_id = True
            elif line.startswith("data: "):
                found_data_field = True
                if last_was_id:
                    id_before_data = True
                last_was_id = False
            elif line == ": ping":
                last_was_id = False
            else:
                last_was_id = False
            
            if found_id_field and found_data_field and id_before_data:
                break
        
        conn.close()
        self.assertTrue(found_id_field, "No 'id:' field found in SSE stream")
        self.assertTrue(found_data_field, "No 'data:' field found in SSE stream")
        self.assertTrue(id_before_data, "'id:' field not found before 'data:' field")
    
    def test_sse_survives_truncation(self):
        """SSE survives truncation: write 2 events, connect, truncate events.jsonl to empty, append new event, assert received."""
        # First, write 2 more events to the existing file (which already has 3)
        run_events = self.swarm_dir / "20261121-125023" / "events.jsonl"
        for i in range(2):
            ev = {"ts": time.time(), "run": "20261121-125023", "task": "task1", "type": "progress", "chars": 200 + i * 10, "model": "model-x"}
            with open(run_events, "a") as f:
                f.write(json.dumps(ev) + "\n")
        
        # Connect to SSE
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        conn.request("GET", "/api/events", headers={"Accept": "text/event-stream"})
        resp = conn.getresponse()
        self.assertEqual(resp.status, 200)
        
        # Wait for initial catch-up (read any existing events)
        time.sleep(0.5)
        
        # Now truncate the file to empty
        with open(run_events, "w") as f:
            pass  # truncate to empty
        
        # Append a new event after truncation
        new_event = {"ts": time.time(), "run": "20261121-125023", "task": "task1", "type": "progress", "chars": 999, "model": "model-x"}
        with open(run_events, "a") as f:
            f.write(json.dumps(new_event) + "\n")
        
        # Read from SSE stream and look for the new event
        deadline = time.time() + 5
        received_truncation_event = False
        
        while time.time() < deadline:
            line = resp.readline()
            if not line:
                break
            line = line.decode("utf-8").strip()
            if line.startswith("data: "):
                event_data = json.loads(line[6:])
                if event_data.get("type") == "progress" and event_data.get("chars") == 999:
                    received_truncation_event = True
                    break
            elif line == ": ping":
                continue
        
        conn.close()
        self.assertTrue(received_truncation_event, "Did not receive event after file truncation")
    
    def test_sse_connection_limit(self):
        """SSE cap: set server.sse_limit to 1, second concurrent connection gets 503."""
        # Lower the SSE limit to 1 for this test
        original_limit = self.server.sse_limit
        self.server.sse_limit = 1
        
        try:
            # First connection - should succeed
            conn1 = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
            conn1.request("GET", "/api/events", headers={"Accept": "text/event-stream"})
            resp1 = conn1.getresponse()
            self.assertEqual(resp1.status, 200)
            
            # Second connection - should get 503
            conn2 = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
            conn2.request("GET", "/api/events", headers={"Accept": "text/event-stream"})
            resp2 = conn2.getresponse()
            self.assertEqual(resp2.status, 503)
            body = resp2.read().decode("utf-8")
            data = json.loads(body)
            self.assertIn("error", data)
            
            # Clean up
            conn1.close()
            conn2.close()
        finally:
            # Restore original limit
            self.server.sse_limit = original_limit


if __name__ == "__main__":
    unittest.main()
