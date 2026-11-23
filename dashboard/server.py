"""
Swarm Dashboard Server

A live web dashboard to watch AI worker agents build software.
Python 3.11+ standard library only.
"""

import os
import json
import re
import time
import threading
import mimetypes
import hmac
from pathlib import Path
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from typing import Optional, Dict, List, Any, Generator
from collections import defaultdict
import traceback


# ─── Constants & Configuration (module-level defaults for main() only) ───────
ID_PATTERN = re.compile(r'^[A-Za-z0-9_-][A-Za-z0-9_.-]*$')
DEFAULT_SWARM_DIR = Path(os.environ.get('SWARM_DIR', '.swarm')).resolve()
DEFAULT_HOST = os.environ.get('HOST', '0.0.0.0')
DEFAULT_PORT = int(os.environ.get('PORT', '8787'))
DEFAULT_TOKEN = os.environ.get('DASHBOARD_TOKEN')  # None = no auth required
STATIC_DIR = Path(__file__).parent / 'static'
MAX_FILE_SIZE = 2 * 1024 * 1024  # 2 MB
SSE_POLL_INTERVAL = 0.5
SSE_PING_INTERVAL = 15
EVENTS_TAIL = 300  # last N non-progress events for /api/runs/<run>
DEFAULT_SSE_LIMIT = 32


# ─── Utility Functions ──────────────────────────────────────────────────────

def validate_id(id_str: str) -> bool:
    """Validate run/task ID format."""
    if id_str in ('.', '..'):
        return False
    return bool(ID_PATTERN.match(id_str))


def safe_join(base: Path, *parts: str) -> Optional[Path]:
    """Safely join path parts, ensuring result stays within base."""
    try:
        result = (base.joinpath(*parts)).resolve()
        if result.is_relative_to(base):
            return result
    except (ValueError, RuntimeError):
        pass
    return None


def is_safe_file_path(rel_path: str) -> bool:
    """
    Check if a relative file path is safe: no '..', no absolute path, no backslash.
    This is the FIRST layer of defense before safe_join.
    """
    if not rel_path:
        return False
    # Reject absolute paths
    if rel_path.startswith('/') or (len(rel_path) > 1 and rel_path[1] == ':'):
        return False
    # Reject backslashes
    if '\\' in rel_path:
        return False
    # Reject '..' components
    parts = rel_path.split('/')
    for part in parts:
        if part == '..' or part == '.':
            return False
    return True


def read_jsonl_tolerant(filepath: Path) -> Generator[Dict[str, Any], None, None]:
    """Read JSONL file, yielding valid objects, skipping malformed lines."""
    if not filepath.exists():
        return
    with filepath.open('r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                # Skip malformed lines silently
                continue


def load_events(run_dir: Path) -> List[Dict[str, Any]]:
    """Load all events from a run's events.jsonl, tolerant of partial/malformed lines."""
    events_file = run_dir / 'events.jsonl'
    return list(read_jsonl_tolerant(events_file))


def load_tasks_spec(run_dir: Path) -> Dict[str, Any]:
    """Load tasks.json spec for a run."""
    tasks_file = run_dir / 'tasks.json'
    if tasks_file.exists():
        try:
            with tasks_file.open('r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return {'name': run_dir.name, 'tasks': []}


def load_live_md(run_dir: Path, task_id: str) -> str:
    """Load live.md content for a task."""
    live_file = safe_join(run_dir, task_id, 'live.md')
    if not live_file or not live_file.is_file():
        return ''
    try:
        return live_file.read_text(encoding='utf-8')
    except OSError:
        return ''


def load_task_files(run_dir: Path, task_id: str) -> List[str]:
    """List all staged files for a task (relative paths, sorted)."""
    task_dir = safe_join(run_dir, task_id, 'files')
    if not task_dir or not task_dir.is_dir():
        return []
    files = []
    for f in task_dir.rglob('*'):
        if f.is_file():
            try:
                rel = f.relative_to(task_dir)
                files.append(str(rel))
            except ValueError:
                continue
    return sorted(files)


def load_file_content(run_dir: Path, task_id: str, rel_path: str) -> Optional[str]:
    """Load a single file's content (traversal-safe, max 2MB)."""
    # FIRST layer: reject unsafe path patterns
    if not is_safe_file_path(rel_path):
        return None
    # SECOND layer: safe_join + is_relative_to
    base = run_dir / task_id / 'files'
    safe_path = safe_join(base, rel_path)
    if not safe_path or not safe_path.exists() or not safe_path.is_file():
        return None
    # Reject symlinks
    try:
        if safe_path.is_symlink():
            return None
    except OSError:
        return None
    try:
        if safe_path.stat().st_size > MAX_FILE_SIZE:
            return None
        return safe_path.read_text(encoding='utf-8')
    except (OSError, UnicodeDecodeError):
        return None


def task_exists_in_run(run_dir: Path, task_id: str, replayed: Dict[str, Any]) -> bool:
    """
    Check if a task is part of the run:
    - in tasks.json tasks, OR
    - in run_start tasks, OR
    - has events for it, OR
    - has a <task> directory
    """
    # Check replayed tasks (from tasks.json + run_start + events)
    for task in replayed.get('tasks', []):
        if task['id'] == task_id:
            return True
    # Check for task directory (using safe_join for defense in depth)
    task_dir = safe_join(run_dir, task_id)
    if task_dir and task_dir.exists() and task_dir.is_dir():
        return True
    return False


# ─── Replay Logic (Pure Functions) ──────────────────────────────────────────

def replay(run_dir: Path) -> Dict[str, Any]:
    """
    Replay events for a run to produce current state.
    Returns dict with: id, name, started, ended, tasks (list of TaskState), events (last 300 non-progress).
    """
    events = load_events(run_dir)
    spec = load_tasks_spec(run_dir)
    run_id = run_dir.name

    # Track task states
    task_states: Dict[str, Dict[str, Any]] = {}
    run_started = None
    run_ended = None

    # Initialize tasks from spec (queued if no events yet)
    for task_spec in spec.get('tasks', []):
        tid = task_spec.get('id')
        if tid and validate_id(tid):
            task_states[tid] = {
                'id': tid,
                'title': task_spec.get('title', tid),
                'status': 'queued',
                'model': None,
                'attempts': [],
                'chars': 0,
                'seconds': 0,  # Will accumulate from task_done/task_failed events
                'prompt_tokens': 0,
                'completion_tokens': 0,
                'tokens': 0,
                'revision': 0,  # Will be set from latest revise_start/task_done/task_failed
                'files': [],
                'notes': '',
                'errors': [],
                'verdict': None,
                'verdict_text': '',
                'applied': False,
                'supervisor_notes': [],
                'feedback': [],
                'started': None,
                'finished': None,
            }

    # Process events in order
    for ev in events:
        ev_type = ev.get('type')
        task_id = ev.get('task')
        ts = ev.get('ts')

        # Tasks added after run_start (e.g. reviewers, QA, auditors from team.py)
        if (task_id and ev_type in ('task_queued', 'task_start') and task_id not in task_states
                and validate_id(task_id)):
            task_states[task_id] = {
                'id': task_id, 'title': ev.get('title', task_id), 'status': 'queued', 'model': None,
                'attempts': [], 'chars': 0, 'seconds': 0, 'prompt_tokens': 0, 'completion_tokens': 0,
                'tokens': 0, 'revision': 0, 'files': [], 'notes': '', 'errors': [], 'verdict': None,
                'verdict_text': '', 'applied': False, 'supervisor_notes': [], 'feedback': [],
                'started': None, 'finished': None,
            }

        if ev_type == 'run_start':
            run_started = ts
            # Add any tasks from run_start not in spec
            for task_spec in ev.get('tasks', []):
                tid = task_spec.get('id')
                if tid and validate_id(tid) and tid not in task_states:
                    task_states[tid] = {
                        'id': tid,
                        'title': task_spec.get('title', tid),
                        'status': 'queued',
                        'model': None,
                        'attempts': [],
                        'chars': 0,
                        'seconds': 0,
                        'prompt_tokens': 0,
                        'completion_tokens': 0,
                        'tokens': 0,
                        'revision': 0,
                        'files': [],
                        'notes': '',
                        'errors': [],
                        'verdict': None,
                        'verdict_text': '',
                        'applied': False,
                        'supervisor_notes': [],
                        'feedback': [],
                        'started': None,
                        'finished': None,
                    }

        elif ev_type == 'run_end':
            run_ended = ts

        elif task_id and task_id in task_states:
            task = task_states[task_id]

            if ev_type == 'task_queued':
                task['status'] = 'queued'
                # Do NOT set started here - only task_start sets started

            elif ev_type == 'task_start':
                # Check if we're in a revision round
                if task.get('revision', 0) > 0 and task['status'] == 'revising':
                    task['status'] = 'revising'
                else:
                    task['status'] = 'running'
                if not task['started']:
                    task['started'] = ts

            elif ev_type == 'progress':
                task['chars'] = ev.get('chars', 0)
                if ev.get('model'):
                    task['model'] = ev['model']

            elif ev_type == 'attempt':
                model = ev.get('model')
                if model:
                    task['attempts'].append({'model': model, 'error': None})
                    task['model'] = model

            elif ev_type == 'attempt_error':
                model = ev.get('model')
                error = ev.get('error', '')
                # Find last attempt with this model and set error
                for attempt in reversed(task['attempts']):
                    if attempt['model'] == model and attempt['error'] is None:
                        attempt['error'] = error
                        break

            elif ev_type == 'task_done':
                task['status'] = 'done'
                task['finished'] = ts
                # Accumulate seconds from event (SUM across revisions)
                task['seconds'] += ev.get('seconds', 0)
                # Accumulate usage
                usage = ev.get('usage', {})
                task['prompt_tokens'] += usage.get('prompt_tokens', 0)
                task['completion_tokens'] += usage.get('completion_tokens', 0)
                task['tokens'] = task['prompt_tokens'] + task['completion_tokens']
                # Revision from latest task_done/task_failed/revise_start
                if 'revision' in ev:
                    task['revision'] = ev['revision']
                task['files'] = ev.get('files', [])
                task['notes'] = ev.get('notes', '')
                task['errors'] = ev.get('errors', [])
                if ev.get('model'):
                    task['model'] = ev['model']

            elif ev_type == 'task_failed':
                task['status'] = 'failed'
                task['finished'] = ts
                # Accumulate seconds from event (SUM across revisions)
                task['seconds'] += ev.get('seconds', 0)
                # May have usage fields like task_done
                usage = ev.get('usage', {})
                if usage:
                    task['prompt_tokens'] += usage.get('prompt_tokens', 0)
                    task['completion_tokens'] += usage.get('completion_tokens', 0)
                    task['tokens'] = task['prompt_tokens'] + task['completion_tokens']
                # Revision from latest task_done/task_failed/revise_start
                if 'revision' in ev:
                    task['revision'] = ev['revision']
                task['files'] = ev.get('files', [])
                task['notes'] = ev.get('notes', '')
                task['errors'] = ev.get('errors', [])
                if ev.get('model'):
                    task['model'] = ev['model']

            elif ev_type == 'revise_start':
                task['status'] = 'revising'
                task['chars'] = 0
                # Revision from latest revise_start
                if 'revision' in ev:
                    task['revision'] = ev['revision']
                task['feedback'].append({
                    'ts': ts,
                    'text': ev.get('feedback', ''),
                    'revision': task['revision']
                })

            elif ev_type == 'verdict':
                task['verdict'] = ev.get('verdict')
                task['verdict_text'] = ev.get('text', '')

            elif ev_type == 'apply':
                task['applied'] = True
                task['files'] = ev.get('files', [])

            elif ev_type == 'supervisor_note':
                task['supervisor_notes'].append({'ts': ts, 'text': ev.get('text', '')})

    # Build task list in spec order, then any extra
    ordered_tasks = []
    seen = set()
    for task_spec in spec.get('tasks', []):
        tid = task_spec.get('id')
        if tid and tid in task_states:
            ordered_tasks.append(task_states[tid])
            seen.add(tid)
    for tid, task in task_states.items():
        if tid not in seen:
            ordered_tasks.append(task)

    # Count statuses
    counts = {'queued': 0, 'running': 0, 'revising': 0, 'done': 0, 'failed': 0}
    for task in ordered_tasks:
        counts[task['status']] = counts.get(task['status'], 0) + 1

    # Total tokens for run
    total_tokens = sum(t['tokens'] for t in ordered_tasks)

    # Filter out 'progress' events for the API response, take last 300
    non_progress_events = [e for e in events if e.get('type') != 'progress']
    tail_events = non_progress_events[-EVENTS_TAIL:]

    return {
        'id': run_id,
        'name': spec.get('name', run_id),
        'started': run_started,
        'ended': run_ended,
        'tasks': ordered_tasks,
        'events': tail_events,
        'counts': counts,
        'tokens': total_tokens,
        'task_count': len(ordered_tasks),
    }


def run_summary(run_dir: Path) -> Optional[Dict[str, Any]]:
    """Generate a RunSummary for /api/runs listing."""
    if not validate_id(run_dir.name):
        return None
    try:
        replayed = replay(run_dir)
        return {
            'id': replayed['id'],
            'name': replayed['name'],
            'started': replayed['started'],
            'ended': replayed['ended'],
            'task_count': replayed['task_count'],
            'counts': replayed['counts'],
            'tokens': replayed['tokens'],
        }
    except Exception:
        return None


def compute_stats(all_runs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Compute aggregate statistics across all runs."""
    total_tokens = 0
    total_prompt = 0
    total_completion = 0
    tasks_done = 0
    tasks_failed = 0
    active_tasks = 0
    models: Dict[str, Dict[str, int]] = defaultdict(lambda: {'tasks': 0, 'tokens': 0})

    for run in all_runs:
        total_tokens += run.get('tokens', 0)
        for task in run.get('tasks', []):
            status = task.get('status')
            if status == 'done':
                tasks_done += 1
            elif status == 'failed':
                tasks_failed += 1
            elif status in ('running', 'revising'):
                active_tasks += 1

            model = task.get('model')
            if model:
                models[model]['tasks'] += 1
                models[model]['tokens'] += task.get('tokens', 0)

            total_prompt += task.get('prompt_tokens', 0)
            total_completion += task.get('completion_tokens', 0)

    return {
        'total_tokens': total_tokens,
        'prompt_tokens': total_prompt,
        'completion_tokens': total_completion,
        'runs': len(all_runs),
        'tasks_done': tasks_done,
        'tasks_failed': tasks_failed,
        'active_tasks': active_tasks,
        'models': dict(models),
    }


# ─── HTTP Request Handler ───────────────────────────────────────────────────

class DashboardHandler(BaseHTTPRequestHandler):
    """HTTP request handler for the dashboard server."""

    # Suppress default log_message (we'll log errors only)
    def log_message(self, format: str, *args) -> None:
        pass

    def log_error(self, format: str, *args) -> None:
        # Only log actual errors, not routine requests
        super().log_error(format, *args)

    # ─── Helpers ────────────────────────────────────────────────────────────

    def _send_security_headers(self) -> None:
        """Send standard security headers on every response."""
        self.send_header('X-Content-Type-Options', 'nosniff')
        self.send_header('Referrer-Policy', 'no-referrer')
        self.send_header('X-Frame-Options', 'DENY')
        self.send_header(
            'Content-Security-Policy',
            "default-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'"
        )

    def _send_json(self, status: int, data: Dict[str, Any]) -> None:
        """Send JSON response with no-store cache control and security headers."""
        body = json.dumps(data, ensure_ascii=False).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Content-Length', str(len(body)))
        self._send_security_headers()
        self.end_headers()
        self.wfile.write(body)

    def _send_text(self, status: int, text: str, content_type: str = 'text/plain; charset=utf-8') -> None:
        """Send text response with security headers."""
        body = text.encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Content-Length', str(len(body)))
        self._send_security_headers()
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, filepath: Path) -> bool:
        """Serve a static file. Returns True if served, False if not found."""
        try:
            if not filepath.exists() or not filepath.is_file():
                return False
            # Security: ensure within static dir
            if not filepath.is_relative_to(STATIC_DIR):
                return False

            mime_type, _ = mimetypes.guess_type(str(filepath))
            if not mime_type:
                mime_type = 'application/octet-stream'

            self.send_response(200)
            self.send_header('Content-Type', mime_type)
            self.send_header('Cache-Control', 'no-store')
            fs = filepath.stat()
            self.send_header('Content-Length', str(fs.st_size))
            self._send_security_headers()
            self.end_headers()

            with filepath.open('rb') as f:
                while chunk := f.read(8192):
                    self.wfile.write(chunk)
            return True
        except (OSError, BrokenPipeError, ConnectionResetError):
            return False

    def _check_auth(self) -> bool:
        """Check token authentication for /api/* routes. Returns True if authorized."""
        token = self.server.dashboard_token
        if not token:
            return True
        # Check query parameter (for EventSource which cannot set headers)
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        query_token = query.get('token', [''])[0]
        if query_token and hmac.compare_digest(query_token, token):
            return True
        # Check Authorization header
        auth = self.headers.get('Authorization', '')
        if auth.startswith('Bearer '):
            provided = auth[7:]
            if hmac.compare_digest(provided, token):
                return True
        return False

    def _require_auth(self) -> bool:
        """Enforce auth on API routes. Returns True if authorized, False if sent 401."""
        if not self._check_auth():
            self._send_json(401, {'error': 'Unauthorized'})
            return False
        return True

    def _get_swarm_dir(self) -> Path:
        """Get the swarm directory from server config."""
        return self.server.swarm_dir

    def _get_replayed_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Get replayed run data, or None if not found/invalid."""
        if not validate_id(run_id):
            return None
        swarm_dir = self._get_swarm_dir()
        run_dir = safe_join(swarm_dir, run_id)
        if not run_dir or not run_dir.exists():
            return None
        try:
            return replay(run_dir)
        except Exception:
            return None

    # ─── Route Handlers ─────────────────────────────────────────────────────

    def _handle_root(self) -> None:
        """GET / -> serve index.html"""
        index_file = STATIC_DIR / 'index.html'
        if not self._send_file(index_file):
            self._send_json(404, {'error': 'Dashboard not built'})

    def _handle_static(self, parts: List[str]) -> None:
        """GET /static/<path> -> serve static file"""
        if len(parts) < 2:
            self._send_json(404, {'error': 'Not found'})
            return
        rel_path = Path(*parts[1:])
        filepath = safe_join(STATIC_DIR, *rel_path.parts)
        if not filepath or not self._send_file(filepath):
            self._send_json(404, {'error': 'Not found'})

    def _handle_healthz(self) -> None:
        """GET /healthz -> unauthenticated health check"""
        self._send_json(200, {'ok': True})

    def _handle_api_runs(self) -> None:
        """GET /api/runs -> list all runs newest first"""
        if not self._require_auth():
            return

        swarm_dir = self._get_swarm_dir()
        runs = []
        if swarm_dir.exists():
            for entry in sorted(swarm_dir.iterdir(), key=lambda e: e.name, reverse=True):
                if entry.is_dir() and validate_id(entry.name):
                    summary = run_summary(entry)
                    if summary:
                        runs.append(summary)

        self._send_json(200, {'runs': runs})

    def _handle_api_run(self, run_id: str) -> None:
        """GET /api/runs/<run> -> full run state"""
        if not self._require_auth():
            return
        replayed = self._get_replayed_run(run_id)
        if not replayed:
            self._send_json(404, {'error': 'Run not found'})
            return
        self._send_json(200, replayed)

    def _handle_task_endpoint(self, run_id: str, task_id: str, handler) -> None:
        """
        Common handler for /live, /files, /file endpoints.
        Validates run and task existence, then calls the specific handler.
        """
        if not self._require_auth():
            return
        if not validate_id(run_id) or not validate_id(task_id):
            self._send_json(404, {'error': 'Invalid ID'})
            return

        replayed = self._get_replayed_run(run_id)
        if not replayed:
            self._send_json(404, {'error': 'Run not found'})
            return

        swarm_dir = self._get_swarm_dir()
        run_dir = safe_join(swarm_dir, run_id)
        if not run_dir:
            self._send_json(404, {'error': 'Run not found'})
            return

        # Check if task is part of this run
        if not task_exists_in_run(run_dir, task_id, replayed):
            self._send_json(404, {'error': 'Task not found'})
            return

        # Call the specific handler
        handler(run_dir, task_id)

    def _handle_api_live(self, run_dir: Path, task_id: str) -> None:
        """GET /api/runs/<run>/tasks/<task>/live -> live.md content"""
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        tail = query.get('tail', [None])[0]

        content = load_live_md(run_dir, task_id)
        if tail and tail.isdigit():
            lines = content.splitlines()
            content = '\n'.join(lines[-int(tail):])

        self._send_text(200, content)

    def _handle_api_files(self, run_dir: Path, task_id: str) -> None:
        """GET /api/runs/<run>/tasks/<task>/files -> list of files"""
        files = load_task_files(run_dir, task_id)
        self._send_json(200, {'files': files})

    def _handle_api_file(self, run_dir: Path, task_id: str) -> None:
        """GET /api/runs/<run>/tasks/<task>/file?path=<rel> -> file content"""
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        rel_path = query.get('path', [''])[0]

        if not rel_path:
            self._send_json(400, {'error': 'Missing path parameter'})
            return

        content = load_file_content(run_dir, task_id, rel_path)
        if content is None:
            self._send_json(404, {'error': 'File not found'})
            return

        self._send_text(200, content)

    def _handle_api_stats(self) -> None:
        """GET /api/stats -> aggregate statistics"""
        if not self._require_auth():
            return

        swarm_dir = self._get_swarm_dir()
        all_runs = []
        if swarm_dir.exists():
            for entry in swarm_dir.iterdir():
                if entry.is_dir() and validate_id(entry.name):
                    try:
                        all_runs.append(replay(entry))
                    except Exception:
                        continue

        stats = compute_stats(all_runs)
        self._send_json(200, stats)

    def _handle_api_events(self) -> None:
        """GET /api/events -> Server-Sent Events stream"""
        if not self._require_auth():
            return

        # Check SSE connection limit
        with self.server.sse_lock:
            if self.server.sse_count >= self.server.sse_limit:
                self._send_json(503, {'error': 'Too many SSE connections'})
                return
            self.server.sse_count += 1

        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream; charset=utf-8')
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Connection', 'keep-alive')
        self._send_security_headers()
        self.end_headers()

        swarm_dir = self._get_swarm_dir()
        # Track byte offset per events.jsonl file
        offsets: Dict[Path, int] = {}
        # Track inode/device for rotation detection
        file_identity: Dict[Path, tuple] = {}
        last_ping = time.time()

        try:
            while True:
                # Discover run directories
                seen_files = set()
                if swarm_dir.exists():
                    for entry in swarm_dir.iterdir():
                        if not entry.is_dir() or not validate_id(entry.name):
                            continue
                        events_file = entry / 'events.jsonl'
                        if not events_file.exists():
                            continue

                        seen_files.add(events_file)

                        # Initialize offset if new
                        if events_file not in offsets:
                            try:
                                st = events_file.stat()
                                offsets[events_file] = st.st_size
                                file_identity[events_file] = (st.st_ino, st.st_dev)
                            except OSError:
                                offsets[events_file] = 0
                                file_identity[events_file] = (0, 0)
                            continue

                        # Read new data
                        try:
                            st = events_file.stat()
                            current_size = st.st_size
                            current_identity = (st.st_ino, st.st_dev)
                            offset = offsets[events_file]

                            # Detect file rotation/replacement (inode or device changed)
                            if file_identity.get(events_file) != current_identity:
                                offset = 0
                                file_identity[events_file] = current_identity

                            # Handle truncation
                            if current_size < offset:
                                offset = 0

                            if current_size > offset:
                                with events_file.open('r', encoding='utf-8') as f:
                                    f.seek(offset)
                                    for line in f:
                                        line = line.strip()
                                        if line:
                                            try:
                                                json.loads(line)  # validate
                                                # Send with id: timestamp and data: json
                                                event_id = str(int(time.time() * 1000))
                                                self.wfile.write(f'id: {event_id}\ndata: {line}\n\n'.encode('utf-8'))
                                            except json.JSONDecodeError:
                                                pass
                                offsets[events_file] = current_size
                        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                            # Client disconnected
                            return
                        except OSError:
                            # File error (e.g., deleted) - drop offset, continue
                            if events_file in offsets:
                                del offsets[events_file]
                            if events_file in file_identity:
                                del file_identity[events_file]
                            continue

                # Prune offsets for files that no longer exist
                for stale in list(offsets.keys()):
                    if stale not in seen_files:
                        del offsets[stale]
                        if stale in file_identity:
                            del file_identity[stale]

                # Send ping every 15s
                now = time.time()
                if now - last_ping >= SSE_PING_INTERVAL:
                    try:
                        self.wfile.write(b': ping\n\n')
                        last_ping = now
                    except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                        return

                time.sleep(SSE_POLL_INTERVAL)

        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            # Client disconnected - silent exit
            pass
        except Exception:
            # Any other error - silent exit for SSE
            pass
        finally:
            with self.server.sse_lock:
                self.server.sse_count -= 1

    def _handle_404(self) -> None:
        """Handle unknown routes."""
        self._send_json(404, {'error': 'Not found'})

    # ─── Main Request Dispatch ──────────────────────────────────────────────

    def do_GET(self) -> None:
        """Handle GET requests."""
        parsed = urlparse(self.path)
        path = parsed.path

        try:
            # Health check (unauthenticated, before other routes)
            if path == '/healthz':
                self._handle_healthz()
                return

            # Static files and root
            if path == '/' or path == '':
                self._handle_root()
                return

            if path.startswith('/static/'):
                parts = [p for p in path.split('/') if p]
                self._handle_static(parts)
                return

            # API routes
            if path.startswith('/api/'):
                api_path = path[5:]  # strip /api
                parts = [p for p in api_path.split('/') if p]

                if not parts:
                    self._handle_404()
                    return

                # /api/runs
                if parts[0] == 'runs':
                    if len(parts) == 1:
                        self._handle_api_runs()
                        return
                    # /api/runs/<run>
                    run_id = parts[1]
                    if len(parts) == 2:
                        self._handle_api_run(run_id)
                        return
                    # /api/runs/<run>/tasks/<task>/...
                    if len(parts) >= 4 and parts[2] == 'tasks':
                        task_id = parts[3]
                        if len(parts) == 4:
                            self._handle_404()
                            return
                        sub = parts[4]
                        if sub == 'live':
                            self._handle_task_endpoint(run_id, task_id, self._handle_api_live)
                            return
                        if sub == 'files':
                            self._handle_task_endpoint(run_id, task_id, self._handle_api_files)
                            return
                        if sub == 'file':
                            self._handle_task_endpoint(run_id, task_id, self._handle_api_file)
                            return

                # /api/stats
                if parts[0] == 'stats' and len(parts) == 1:
                    self._handle_api_stats()
                    return

                # /api/events
                if parts[0] == 'events' and len(parts) == 1:
                    self._handle_api_events()
                    return

                self._handle_404()
                return

            # Unknown route
            self._handle_404()

        except (BrokenPipeError, ConnectionResetError):
            # Client disconnected during request handling
            pass
        except Exception as e:
            # Log unexpected errors
            self.log_error("Unhandled error: %s", traceback.format_exc())
            try:
                self._send_json(500, {'error': 'Internal server error'})
            except (BrokenPipeError, ConnectionResetError):
                pass


# ─── Server Factory & Entry Point ───────────────────────────────────────────

def make_server(host: str = DEFAULT_HOST, port: int = DEFAULT_PORT,
                swarm_dir: Optional[str | Path] = None,
                token: Optional[str] = None,
                sse_limit: int = DEFAULT_SSE_LIMIT) -> ThreadingHTTPServer:
    """
    Create and configure the dashboard server (not started).
    Args:
        host: Bind address
        port: Port number
        swarm_dir: Override SWARM_DIR (str or Path). If None, uses env default.
        token: Override DASHBOARD_TOKEN (for tests). If None, uses env default.
        sse_limit: Max concurrent SSE connections (default 32).
    Returns:
        ThreadingHTTPServer instance (call serve_forever() to start)
    """
    # Resolve swarm_dir: use provided, else env default
    if swarm_dir is not None:
        resolved_swarm_dir = Path(swarm_dir).resolve()
    else:
        resolved_swarm_dir = DEFAULT_SWARM_DIR

    # Resolve token: use provided, else env default
    resolved_token = token if token is not None else DEFAULT_TOKEN

    # Ensure static directory exists
    STATIC_DIR.mkdir(parents=True, exist_ok=True)

    server = ThreadingHTTPServer((host, port), DashboardHandler)
    server.daemon_threads = True
    # Store config on server for handler access (NOT module globals)
    server.swarm_dir = resolved_swarm_dir
    server.dashboard_token = resolved_token
    server.sse_limit = sse_limit
    server.sse_count = 0
    server.sse_lock = threading.Lock()
    return server


def main() -> None:
    """Entry point: start the server and serve forever."""
    server = make_server()
    print(f"Swarm Dashboard listening on http://{DEFAULT_HOST}:{DEFAULT_PORT}")
    print(f"SWARM_DIR: {DEFAULT_SWARM_DIR}")
    if DEFAULT_TOKEN:
        print("Token authentication: ENABLED")
    else:
        print("Token authentication: DISABLED (set DASHBOARD_TOKEN to enable)")
    print(f"SSE connection limit: {DEFAULT_SSE_LIMIT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        server.server_close()


if __name__ == '__main__':
    main()
