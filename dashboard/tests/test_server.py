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


class TestDashboardServer(unittest.TestCase):
    """Integration tests for the dashboard server."""

    @classmethod
    def setUpClass(cls):
        # Create temporary SWARM_DIR with test data
        cls.tmpdir = tempfile.mkdtemp(prefix="swarm_test_")
        cls.swarm_dir = Path(cls.tmpdir)
        cls._create_test_runs()
        
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
        cls.tmpdir2 = tempfile.mkdtemp(prefix="swarm_test_auth_")
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
    def _create_test_runs(cls):
        """Create run A (finished) and run B (in progress)."""
        # Run A - fully finished
        run_a = cls.swarm_dir / "20261121-125023"
        run_a.mkdir(parents=True)
        task1_dir = run_a / "task1"
        task1_dir.mkdir()
        task2_dir = run_a / "task2"
        task2_dir.mkdir()
        
        # tasks.json for run A
        tasks_a = {
            "name": "Run A - Complete",
            "tasks": [
                {"id": "task1", "title": "First Task", "instructions": "Do something"},
                {"id": "task2", "title": "Second Task", "instructions": "Do another thing"}
            ]
        }
        (run_a / "tasks.json").write_text(json.dumps(tasks_a))
        
        # events.jsonl for run A
        # Revision numbering starts at 0: first task_done is revision 0, revise_start/second task_done is revision 1
        events_a = [
            {"ts": 1700000000.0, "run": "20261121-125023", "task": None, "type": "run_start", "name": "Run A - Complete", "tasks": [{"id": "task1", "title": "First Task"}, {"id": "task2", "title": "Second Task"}]},
            {"ts": 1700000001.0, "run": "20261121-125023", "task": "task1", "type": "task_queued", "title": "First Task"},
            {"ts": 1700000002.0, "run": "20261121-125023", "task": "task2", "type": "task_queued", "title": "Second Task"},
            {"ts": 1700000003.0, "run": "20261121-125023", "task": "task1", "type": "task_start", "title": "First Task"},
            {"ts": 1700000004.0, "run": "20261121-125023", "task": "task1", "type": "attempt", "model": "model-a"},
            {"ts": 1700000005.0, "run": "20261121-125023", "task": "task1", "type": "attempt_error", "model": "model-a", "error": "Rate limited"},
            {"ts": 1700000006.0, "run": "20261121-125023", "task": "task1", "type": "attempt", "model": "model-b"},
            {"ts": 1700000007.0, "run": "20261121-125023", "task": "task1", "type": "progress", "chars": 1500, "model": "model-b"},
            {"ts": 1700000008.0, "run": "20261121-125023", "task": "task1", "type": "task_done", "model": "model-b", "seconds": 10.5, "usage": {"prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150}, "files": ["output.txt"], "rejected": [], "notes": "Done", "errors": [], "revision": 0},
            {"ts": 1700000009.0, "run": "20261121-125023", "task": "task1", "type": "revise_start", "feedback": "Please improve", "revision": 1},
            {"ts": 1700000010.0, "run": "20261121-125023", "task": "task1", "type": "task_start", "title": "First Task"},
            {"ts": 1700000011.0, "run": "20261121-125023", "task": "task1", "type": "task_done", "model": "model-b", "seconds": 5.0, "usage": {"prompt_tokens": 20, "completion_tokens": 10, "total_tokens": 30}, "files": ["output_v2.txt"], "rejected": [], "notes": "Revised", "errors": [], "revision": 1},
            {"ts": 1700000012.0, "run": "20261121-125023", "task": "task1", "type": "verdict", "verdict": "approved", "text": "Looks good"},
            {"ts": 1700000013.0, "run": "20261121-125023", "task": "task1", "type": "apply", "files": ["output_v2.txt"]},
            {"ts": 1700000014.0, "run": "20261121-125023", "task": "task1", "type": "supervisor_note", "text": "Approved and applied"},
            {"ts": 1700000015.0, "run": "20261121-125023", "task": None, "type": "run_end"},
            # Malformed line
            "not valid json at all",
        ]
        with open(run_a / "events.jsonl", "w") as f:
            for ev in events_a:
                if isinstance(ev, str):
                    f.write(ev + "\n")
                else:
                    f.write(json.dumps(ev) + "\n")
        
        # live.md for task1 (revision 1)
        live_text = "Line 1\nLine 2\nLine 3\nLine 4\nLine 5\n" * 10
        (task1_dir / "live.md").write_text(live_text)
        
        # Files for task1
        (task1_dir / "files").mkdir()
        (task1_dir / "files" / "output.txt").write_text("First version output")
        (task1_dir / "files" / "output_v2.txt").write_text("Revised version output")
        
        # result.json for task1
        (task1_dir / "result.json").write_text(json.dumps({"status": "done"}))
        
        # Run B - in progress
        run_b = cls.swarm_dir / "20261121-130000"
        run_b.mkdir(parents=True)
        task3_dir = run_b / "task3"
        task3_dir.mkdir()
        task4_dir = run_b / "task4"
        task4_dir.mkdir()
        
        tasks_b = {
            "name": "Run B - In Progress",
            "tasks": [
                {"id": "task3", "title": "Running Task", "instructions": "Stream output"},
                {"id": "task4", "title": "Queued Task", "instructions": "Waiting"}
            ]
        }
        (run_b / "tasks.json").write_text(json.dumps(tasks_b))
        
        events_b = [
            {"ts": 1700001000.0, "run": "20261121-130000", "task": None, "type": "run_start", "name": "Run B - In Progress", "tasks": [{"id": "task3", "title": "Running Task"}, {"id": "task4", "title": "Queued Task"}]},
            {"ts": 1700001001.0, "run": "20261121-130000", "task": "task3", "type": "task_queued", "title": "Running Task"},
            {"ts": 1700001002.0, "run": "20261121-130000", "task": "task3", "type": "task_start", "title": "Running Task"},
            {"ts": 1700001003.0, "run": "20261121-130000", "task": "task3", "type": "attempt", "model": "model-c"},
            {"ts": 1700001004.0, "run": "20261121-130000", "task": "task3", "type": "progress", "chars": 500, "model": "model-c"},
            # task4 only in run_start, no events -> queued
        ]
        with open(run_b / "events.jsonl", "w") as f:
            for ev in events_b:
                f.write(json.dumps(ev) + "\n")
        
        # live.md for task3
        (task3_dir / "live.md").write_text("Streaming output line 1\nStreaming output line 2\nStreaming output line 3\n")
        
        # Files for task3
        (task3_dir / "files").mkdir()
        (task3_dir / "files" / "partial.txt").write_text("Partial output")
        
        # result.json for task3
        (task3_dir / "result.json").write_text(json.dumps({"status": "running"}))

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

    def _assert_content_type(self, headers, expected_type, charset="utf-8"):
        """Assert content type matches expected type and charset."""
        self.assertEqual(headers.get_content_type(), expected_type)
        content_type_header = headers.get("Content-Type", "")
        self.assertIn(f"charset={charset}", content_type_header)

    # ===== /api/runs tests =====
    
    def test_api_runs_ordering_and_counts(self):
        data = self._get_json("/api/runs")
        self.assertIn("runs", data)
        runs = data["runs"]
        self.assertEqual(len(runs), 2)
        # Newest first (run B has later timestamp)
        self.assertEqual(runs[0]["id"], "20261121-130000")
        self.assertEqual(runs[1]["id"], "20261121-125023")
        
        # Run A counts - task2 only has task_queued, never starts -> queued=1
        run_a = runs[1]
        self.assertEqual(run_a["name"], "Run A - Complete")
        self.assertEqual(run_a["task_count"], 2)
        self.assertEqual(run_a["counts"]["done"], 1)
        self.assertEqual(run_a["counts"]["failed"], 0)
        self.assertEqual(run_a["counts"]["queued"], 1)  # task2 never started
        self.assertEqual(run_a["counts"]["running"], 0)
        self.assertEqual(run_a["counts"]["revising"], 0)
        self.assertEqual(run_a["tokens"], 180)  # 150 + 30
        self.assertIsNotNone(run_a["ended"])
        
        # Run B counts
        run_b = runs[0]
        self.assertEqual(run_b["name"], "Run B - In Progress")
        self.assertEqual(run_b["task_count"], 2)
        self.assertEqual(run_b["counts"]["running"], 1)
        self.assertEqual(run_b["counts"]["queued"], 1)
        self.assertEqual(run_b["counts"]["done"], 0)
        self.assertIsNone(run_b["ended"])

    # ===== /api/runs/<run> tests =====
    
    def test_api_run_a_detail(self):
        data = self._get_json("/api/runs/20261121-125023")
        self.assertEqual(data["id"], "20261121-125023")
        self.assertEqual(data["name"], "Run A - Complete")
        self.assertIsNotNone(data["ended"])
        self.assertEqual(len(data["tasks"]), 2)
        
        # Task1 - done, revision 1 (second revision), verdict approved, applied
        task1 = next(t for t in data["tasks"] if t["id"] == "task1")
        self.assertEqual(task1["title"], "First Task")
        self.assertEqual(task1["status"], "done")
        self.assertEqual(task1["model"], "model-b")
        self.assertEqual(task1["revision"], 1)  # Latest revision is 1
        self.assertEqual(task1["tokens"], 180)  # 150 + 30
        self.assertEqual(task1["prompt_tokens"], 120)  # 100 + 20
        self.assertEqual(task1["completion_tokens"], 60)  # 50 + 10
        self.assertEqual(task1["seconds"], 15.5)  # 10.5 + 5.0
        self.assertEqual(task1["chars"], 0)  # revise_start resets chars to 0, no progress after
        self.assertEqual(task1["verdict"], "approved")
        self.assertEqual(task1["verdict_text"], "Looks good")
        self.assertTrue(task1["applied"])
        self.assertEqual(task1["files"], ["output_v2.txt"])  # files from the latest round
        self.assertEqual(task1["notes"], "Revised")  # latest notes
        self.assertEqual(task1["errors"], [])
        self.assertEqual(len(task1["attempts"]), 2)
        self.assertEqual(task1["attempts"][0]["model"], "model-a")
        self.assertEqual(task1["attempts"][0]["error"], "Rate limited")
        self.assertEqual(task1["attempts"][1]["model"], "model-b")
        self.assertIsNone(task1["attempts"][1]["error"])
        self.assertIsNotNone(task1["started"])
        self.assertIsNotNone(task1["finished"])
        
        # Check supervisor_notes
        self.assertEqual(len(task1["supervisor_notes"]), 1)
        self.assertEqual(task1["supervisor_notes"][0]["text"], "Approved and applied")
        
        # Check feedback
        self.assertEqual(len(task1["feedback"]), 1)
        self.assertEqual(task1["feedback"][0]["text"], "Please improve")
        self.assertEqual(task1["feedback"][0]["revision"], 1)
        
        # Task2 - queued (only task_queued event, never started)
        task2 = next(t for t in data["tasks"] if t["id"] == "task2")
        self.assertEqual(task2["status"], "queued")
        self.assertEqual(task2["revision"], 0)  # Never revised, revision 0
        self.assertEqual(task2["tokens"], 0)
        self.assertIsNone(task2["started"])
        self.assertIsNone(task2["finished"])
        
        # Events - last 300, oldest first
        self.assertGreaterEqual(len(data["events"]), 15)  # 15 valid events + malformed skipped
        # First event should be run_start
        self.assertEqual(data["events"][0]["type"], "run_start")
        # Last event should be run_end
        self.assertEqual(data["events"][-1]["type"], "run_end")

    def test_api_run_b_detail(self):
        data = self._get_json("/api/runs/20261121-130000")
        self.assertEqual(data["id"], "20261121-130000")
        self.assertEqual(len(data["tasks"]), 2)
        
        # Task3 - running, revision 0 (never revised)
        task3 = next(t for t in data["tasks"] if t["id"] == "task3")
        self.assertEqual(task3["status"], "running")
        self.assertEqual(task3["model"], "model-c")
        self.assertEqual(task3["chars"], 500)
        self.assertEqual(task3["revision"], 0)  # Never revised
        self.assertIsNotNone(task3["started"])
        self.assertIsNone(task3["finished"])
        
        # Task4 - queued (only in run_start)
        task4 = next(t for t in data["tasks"] if t["id"] == "task4")
        self.assertEqual(task4["status"], "queued")
        self.assertIsNone(task4["started"])

    def test_api_run_invalid_id_404(self):
        status, _, body = self._get("/api/runs/../etc/passwd")
        self.assertEqual(status, 404)
        data = json.loads(body)
        self.assertIn("error", data)
        
        status, _, body = self._get("/api/runs/invalid@id")
        self.assertEqual(status, 404)

    # ===== /api/runs/<run>/tasks/<task>/live tests =====
    
    def test_api_live_full(self):
        status, headers, body = self._get("/api/runs/20261121-125023/tasks/task1/live")
        self.assertEqual(status, 200)
        self._assert_content_type(headers, "text/plain")
        # The fixture writes exactly 10 repetitions of 5 lines
        expected = "Line 1\nLine 2\nLine 3\nLine 4\nLine 5\n" * 10
        self.assertEqual(body, expected)

    def test_api_live_tail(self):
        status, headers, body = self._get("/api/runs/20261121-125023/tasks/task1/live?tail=3")
        self.assertEqual(status, 200)
        self._assert_content_type(headers, "text/plain")
        lines = body.strip().split("\n")
        self.assertEqual(len(lines), 3)
        self.assertEqual(lines[-1], "Line 5")

    def test_api_live_missing_task_404(self):
        status, _, _ = self._get("/api/runs/20261121-125023/tasks/nonexistent/live")
        self.assertEqual(status, 404)

    def test_api_live_missing_file_empty(self):
        # task2 has no live.md
        status, headers, body = self._get("/api/runs/20261121-125023/tasks/task2/live")
        self.assertEqual(status, 200)
        self._assert_content_type(headers, "text/plain")
        self.assertEqual(body, "")

    # ===== /api/runs/<run>/tasks/<task>/files tests =====
    
    def test_api_files_list(self):
        data = self._get_json("/api/runs/20261121-125023/tasks/task1/files")
        self.assertIn("files", data)
        files = data["files"]
        self.assertEqual(sorted(files), sorted(["output.txt", "output_v2.txt"]))

    def test_api_files_empty_for_no_files(self):
        data = self._get_json("/api/runs/20261121-130000/tasks/task4/files")
        self.assertEqual(data["files"], [])

    # ===== /api/runs/<run>/tasks/<task>/file tests =====
    
    def test_api_file_content(self):
        status, headers, body = self._get("/api/runs/20261121-125023/tasks/task1/file?path=output.txt")
        self.assertEqual(status, 200)
        self._assert_content_type(headers, "text/plain")
        self.assertEqual(body, "First version output")

    def test_api_file_traversal_blocked(self):
        # Path traversal attempts - any '..' should return 404
        status, _, _ = self._get("/api/runs/20261121-125023/tasks/task1/file?path=../../etc/passwd")
        self.assertEqual(status, 404)
        
        status, _, _ = self._get("/api/runs/20261121-125023/tasks/task1/file?path=files/../output.txt")
        self.assertEqual(status, 404)
        
        status, _, _ = self._get("/api/runs/20261121-125023/tasks/task1/file?path=output.txt/..")
        self.assertEqual(status, 404)

    def test_api_file_missing_404(self):
        status, _, _ = self._get("/api/runs/20261121-125023/tasks/task1/file?path=missing.txt")
        self.assertEqual(status, 404)

    def test_api_file_run_id_traversal_404(self):
        status, _, _ = self._get("/api/runs/../etc/passwd/tasks/task1/file?path=output.txt")
        self.assertEqual(status, 404)

    def test_api_file_task_id_traversal_404(self):
        status, _, _ = self._get("/api/runs/20261121-125023/tasks/../task1/file?path=output.txt")
        self.assertEqual(status, 404)

    # ===== /api/stats tests =====
    
    def test_api_stats(self):
        data = self._get_json("/api/stats")
        self.assertEqual(data["runs"], 2)
        self.assertEqual(data["tasks_done"], 1)
        self.assertEqual(data["tasks_failed"], 0)
        self.assertEqual(data["active_tasks"], 1)  # task3 running
        self.assertEqual(data["total_tokens"], 180)
        self.assertEqual(data["prompt_tokens"], 120)
        self.assertEqual(data["completion_tokens"], 60)
        self.assertIn("models", data)
        self.assertIn("model-b", data["models"])
        self.assertEqual(data["models"]["model-b"]["tasks"], 1)
        self.assertEqual(data["models"]["model-b"]["tokens"], 180)
        self.assertIn("model-c", data["models"])
        self.assertEqual(data["models"]["model-c"]["tasks"], 1)

    # ===== DASHBOARD_TOKEN auth tests =====
    
    def test_auth_required_without_token(self):
        status, _, body = self._get("/api/runs", base_url=self.base_url_auth)
        self.assertEqual(status, 401)
        data = json.loads(body)
        self.assertIn("error", data)

    def test_auth_with_query_token(self):
        data = self._get_json("/api/runs?token=test-token", base_url=self.base_url_auth)
        self.assertEqual(len(data["runs"]), 1)

    def test_auth_with_header_token(self):
        data = self._get_json("/api/runs", token="test-token", base_url=self.base_url_auth)
        self.assertEqual(len(data["runs"]), 1)

    def test_auth_wrong_token(self):
        status, _, _ = self._get("/api/runs?token=wrong", base_url=self.base_url_auth)
        self.assertEqual(status, 401)
        
        status, _, _ = self._get("/api/runs", token="wrong", base_url=self.base_url_auth)
        self.assertEqual(status, 401)

    def test_auth_static_files_public(self):
        # Static files should be accessible without token even when token is set
        status, headers, body = self._get("/", base_url=self.base_url_auth)
        self.assertEqual(status, 200)
        self.assertIn("Swarm", body)

    def test_auth_no_token_server_still_works(self):
        # Main server has no token, should work without auth
        data = self._get_json("/api/runs")
        self.assertEqual(len(data["runs"]), 2)

    # ===== Static file serving tests =====
    
    def test_static_index_served(self):
        status, headers, body = self._get("/")
        self.assertEqual(status, 200)
        content_type = headers.get("Content-Type", "")
        self.assertTrue(content_type.startswith("text/html"), f"Expected text/html, got {content_type}")
        self.assertIn("Swarm", body)
        self.assertIn('id="stats"', body)
        self.assertIn('id="sidebar"', body)
        self.assertIn('id="main"', body)
        self.assertIn('id="feed"', body)
        self.assertIn('id="task-detail"', body)

    def test_static_css_served(self):
        status, headers, body = self._get("/static/style.css")
        self.assertEqual(status, 200)
        content_type = headers.get("Content-Type", "")
        self.assertTrue(content_type.startswith("text/css"), f"Expected text/css, got {content_type}")

    def test_static_js_served(self):
        status, headers, body = self._get("/static/app.js")
        self.assertEqual(status, 200)
        content_type = headers.get("Content-Type", "")
        self.assertTrue(content_type.startswith("application/javascript") or content_type.startswith("text/javascript"),
                        f"Expected application/javascript or text/javascript, got {content_type}")

    def test_static_traversal_blocked(self):
        status, _, _ = self._get("/static/../../etc/passwd")
        self.assertEqual(status, 404)

    def test_unknown_route_404(self):
        status, _, body = self._get("/api/unknown")
        self.assertEqual(status, 404)
        data = json.loads(body)
        self.assertIn("error", data)

    # ===== SSE /api/events tests =====
    
    def test_sse_receives_new_events(self):
        # Connect to SSE endpoint
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        conn.request("GET", "/api/events", headers={"Accept": "text/event-stream"})
        resp = conn.getresponse()
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.getheader("Content-Type"), "text/event-stream; charset=utf-8")
        
        # Read initial data (should be empty or just ping)
        time.sleep(0.2)
        
        # Append a new event to run B's events.jsonl
        run_b_events = self.swarm_dir / "20261121-130000" / "events.jsonl"
        new_event = {"ts": time.time(), "run": "20261121-130000", "task": "task3", "type": "progress", "chars": 600, "model": "model-c"}
        with open(run_b_events, "a") as f:
            f.write(json.dumps(new_event) + "\n")
        
        # Read from SSE stream with timeout
        deadline = time.time() + 5
        received_data = False
        while time.time() < deadline:
            line = resp.readline()
            if not line:
                break
            line = line.decode("utf-8").strip()
            if line.startswith("data: "):
                event_data = json.loads(line[6:])
                if event_data.get("type") == "progress" and event_data.get("chars") == 600:
                    received_data = True
                    break
            elif line == ": ping":
                continue
        
        conn.close()
        self.assertTrue(received_data, "Did not receive the new event via SSE")

    def test_sse_ping_sent(self):
        # Verify ping is sent periodically
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=20)
        conn.request("GET", "/api/events", headers={"Accept": "text/event-stream"})
        resp = conn.getresponse()
        self.assertEqual(resp.status, 200)
        
        deadline = time.time() + 18
        ping_received = False
        while time.time() < deadline:
            line = resp.readline()
            if not line:
                break
            line = line.decode("utf-8").strip()
            if line == ": ping":
                ping_received = True
                break
        
        conn.close()
        self.assertTrue(ping_received, "Did not receive ping within 18 seconds")


if __name__ == "__main__":
    unittest.main()
