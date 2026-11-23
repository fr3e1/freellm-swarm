#!/usr/bin/env python3
"""swarm.py - farm development tasks out to FreeLLMAPI workers in parallel.

Claude (supervisor) writes a tasks file, runs the workers, reviews the staged
output, sends revisions back, and only then applies files to the project.

  swarm.py models [--all]                 list models the router offers (ready ones by default)
  swarm.py run tasks.json [-j N]          run all tasks in parallel -> .swarm/<run>/
  swarm.py revise <run> <task> "feedback" re-run one task with review feedback
  swarm.py revise-many <run> fb.json      re-run several tasks at once ({"task": "feedback", ...})
  swarm.py sync [run]                     push run data to the dashboard server now
  swarm.py note <run> [task] "text"       log a supervisor note (shown on the dashboard)
  swarm.py verdict <run> <task> approved|rejected ["why"]
  swarm.py status <run>                   summary of a run
  swarm.py apply <run> <task> [--dest D]  copy a task's approved files into the project

Every run writes .swarm/<run>/events.jsonl (one JSON event per line) and each
task streams its in-progress output to .swarm/<run>/<task>/live.md; the
dashboard reads both. While a command runs, the run folder is pushed to the
dashboard server every few seconds (rsync over SSH), so it shows up live at
SWARM_DASHBOARD_URL.

Parallelism: tasks are spread over a pool of models (at most --per-model at a
time on each), so one rate-limited model no longer stalls the whole run. A
model that answers 429 is cooled down and its task moves to the next free
model. Tasks with "depends_on" start as soon as those tasks finish, and they
can read the files those tasks staged via context_files.

tasks.json:
{
  "name": "human readable run name",
  "project": "path/to/project",          # files are read from / applied to here
  "model": "nemotron-3-ultra-550b",      # optional: first choice in the pool
  "model_pool": ["..."],                 # optional: models to spread tasks over (default DEFAULT_POOL)
  "fallback_models": ["..."],            # optional: extra models appended to the pool
  "shared_context": "Stack, conventions, interfaces every worker must follow",
  "tasks": [
    {"id": "api", "title": "short label", "instructions": "...", "context_files": ["src/types.ts"],
     "owns": ["src/api/**"], "model": "optional: pin this task to one model",
     "avoid_models": ["optional: never use these"], "system": "optional: replaces the worker system prompt",
     "depends_on": ["optional: ids that must finish first"]}
  ]
}

Config: FREELLMAPI_URL (default http://192.168.10.122:3001) and FREELLMAPI_KEY,
or the key on one line in ~/.config/freellmapi/key. SWARM_DIR overrides .swarm.
Dashboard: SWARM_DASHBOARD_URL (default http://192.168.10.123:8787/),
SWARM_SYNC rsync target (default swarmsync@192.168.10.123: ; "off" disables),
SWARM_SYNC_SSH ssh command for rsync (e.g. "ssh -i ~/.ssh/id_ed25519").
"""
import argparse, concurrent.futures as cf, fnmatch, json, os, re, shlex, shutil, subprocess, sys, threading, time
import urllib.request, urllib.error
from datetime import datetime
from pathlib import Path

BASE = os.environ.get("FREELLMAPI_URL", "http://192.168.10.122:3001").rstrip("/")
KEY_FILE = Path.home() / ".config/freellmapi/key"
SWARM_DIR = Path(os.environ.get("SWARM_DIR", ".swarm"))
DASHBOARD_URL = os.environ.get("SWARM_DASHBOARD_URL", "http://192.168.10.123:8787/").rstrip("/") + "/"
SYNC_DEST = os.environ.get("SWARM_SYNC", "swarmsync@192.168.10.123:")
SYNC_SSH = os.environ.get("SWARM_SYNC_SSH", "ssh")
SYNC_EVERY = 3.0  # seconds between pushes while a run is live

# Strong coding models on the router, best first. Only the ones the router
# currently reports as usable are kept at run start.
DEFAULT_POOL = ["nemotron-3-ultra-550b", "nemotron-3.5-super-vl", "kilo-auto", "gpt-oss-120b", "qwen3.5-397b",
                "qwen3.8-27b", "llama-3.3-70b", "free-router", "nemotron-3.5-lightning", "auto"]
MAX_CHARS = int(os.environ.get("SWARM_MAX_CHARS", "200000"))  # abort a worker past this many chars
COOLDOWN = (int(os.environ.get("SWARM_COOLDOWN", "60")), 600)  # first / max seconds a model rests after a 429

SYSTEM = """You are a senior software engineer working as one member of a parallel team.
A supervisor splits the project into tasks, reviews your output strictly, and may send it back.

Rules:
- Do ONLY your assigned task. Only create/modify files you are allowed to own.
- Output every file you create or change IN FULL (no placeholders, no "rest unchanged", no TODO stubs).
- Follow the shared context and interfaces exactly; other workers depend on them.
- Write production-quality, runnable code with error handling.

Output format (strict):
<file path="relative/path/from/project/root">
...complete file contents...
</file>
(repeat for each file)
<notes>
Short summary: what you did, assumptions, anything the supervisor must check or wire up.
</notes>"""

# Lenient: tolerates a missing/odd quote around the path and a missing </file> before the next block.
# Tags only count at the start of a line, so code that mentions "<file path=" in a string
# (tests, prompts) doesn't cut a file short.
FILE_RE = re.compile(r'^[ \t]*<file\s+path=["\']?([^"\'>\n]+?)["\']?\s*>[ \t]*\n?(.*?)\n?'
                     r'(?:^[ \t]*</file>|(?=^[ \t]*<file\s+path=)|(?=^[ \t]*<notes>)|\Z)', re.S | re.M)
NOTES_RE = re.compile(r"<notes>(.*?)</notes>", re.S)
_lock = threading.Lock()


def emit(run_dir, type_, task=None, **data):
    ev = {"ts": round(time.time(), 3), "run": run_dir.name, "task": task, "type": type_, **data}
    with _lock, open(run_dir / "events.jsonl", "a") as f:
        f.write(json.dumps(ev) + "\n")
    if run_dir.name in _syncers:
        _syncers[run_dir.name].poke()


def api_key():
    k = os.environ.get("FREELLMAPI_KEY")
    if not k and KEY_FILE.exists():
        k = KEY_FILE.read_text().strip()
    if not k:
        sys.exit(f"No API key: set FREELLMAPI_KEY or put it in {KEY_FILE}")
    return k


def request(method, path, body=None):
    return urllib.request.Request(BASE + path, method=method,
                                  data=json.dumps(body).encode() if body else None,
                                  headers={"Authorization": f"Bearer {api_key()}",
                                           "Content-Type": "application/json"})


def http(method, path, body=None, timeout=600):
    with urllib.request.urlopen(request(method, path, body), timeout=timeout) as r:
        return json.loads(r.read())


def runaway(text):
    """True when a worker is looping (repeating a big chunk) or far past any sane answer size."""
    if len(text) > MAX_CHARS:
        return True
    if len(text) > 40_000:
        tail = text[-2_000:]
        return text.count(tail) > 1
    return False


def chat_stream(model, messages, live_path, on_progress):
    """Stream a completion, mirroring partial output into live_path."""
    body = {"model": model, "messages": messages, "temperature": 0.2,
            "stream": True, "stream_options": {"include_usage": True}}
    t, buf, used, usage, last_flush = time.time(), [], model, {}, 0.0
    live_path.write_text("")
    with urllib.request.urlopen(request("POST", "/v1/chat/completions", body), timeout=600) as r:
        if "event-stream" not in r.headers.get("Content-Type", ""):
            j = json.loads(r.read())  # router answered without streaming
            text = j["choices"][0]["message"].get("content") or ""
            live_path.write_text(text)
            return text, j.get("model", model), round(time.time() - t, 1), j.get("usage", {})
        for raw in r:
            line = raw.decode(errors="replace").strip()
            if not line.startswith("data:"):
                continue
            data = line[5:].strip()
            if data == "[DONE]":
                break
            try:
                j = json.loads(data)
            except ValueError:
                continue
            if j.get("error"):
                raise RuntimeError(f"stream error: {json.dumps(j['error'])[:300]}")
            used = j.get("model") or used
            usage = j.get("usage") or usage
            for ch in j.get("choices") or []:
                piece = (ch.get("delta") or {}).get("content")
                if piece:
                    buf.append(piece)
            if time.time() - last_flush > 0.5:
                text = "".join(buf)
                live_path.write_text(text)
                on_progress(len(text), used)
                last_flush = time.time()
                if runaway(text):
                    raise RuntimeError(f"runaway output ({len(text)} chars, repeating or over the limit)")
    text = "".join(buf)
    live_path.write_text(text)
    if not text.strip():
        raise RuntimeError("empty response")
    return text, used, round(time.time() - t, 1), usage


class RateLimited(RuntimeError):
    """The router has no capacity left for this model right now."""


def chat(model, messages, live_path, on_progress, retries=2):
    last = None
    for attempt in range(retries):
        try:
            return chat_stream(model, messages, live_path, on_progress)
        except urllib.error.HTTPError as e:
            last = f"HTTP {e.code}: {e.read().decode(errors='replace')[:300]}"
            if e.code == 429:  # router already tried every route for this model
                raise RateLimited(last)
            if e.code in (400, 401, 403, 404):
                break
        except Exception as e:  # timeouts, bad json, network
            last = f"{type(e).__name__}: {e}"
            if "429" in last or "exhausted" in last.lower():
                raise RateLimited(last)
        time.sleep(2 * (attempt + 1))
    raise RuntimeError(last)


class ModelPool:
    """Hands out models so parallel tasks spread over several of them.

    acquire() returns the most preferred model that has a free slot, is not
    cooling down after a 429 and is not in `skip`; it blocks while every such
    model is busy or cooling, and returns None once nothing is left to try.
    """

    def __init__(self, models, per_model=2, cooling=()):
        self.models = list(dict.fromkeys(m for m in models if m))
        self.per_model = max(1, per_model)
        self.busy = {m: 0 for m in self.models}
        self.cool_until, self.strikes = {}, {}
        self.cv = threading.Condition()
        for m in cooling:
            self.cool(m)

    def acquire(self, skip=(), prefer=None):
        order = ([prefer] if prefer in self.busy else []) + self.models
        with self.cv:
            while True:
                left = [m for m in dict.fromkeys(order) if m not in skip]
                if not left:
                    return None
                now = time.time()
                for m in left:
                    if self.busy[m] < self.per_model and self.cool_until.get(m, 0) <= now:
                        self.busy[m] += 1
                        return m
                waits = [self.cool_until[m] - now for m in left if self.cool_until.get(m, 0) > now]
                self.cv.wait(timeout=max(0.5, min(waits)) if waits else None)

    def release(self, model, ok=True):
        with self.cv:
            if model in self.busy:
                self.busy[model] -= 1
                if ok:
                    self.strikes.pop(model, None)
            self.cv.notify_all()

    def cool(self, model):
        with self.cv:
            n = self.strikes[model] = self.strikes.get(model, 0) + 1
            secs = min(COOLDOWN[0] * 2 ** (n - 1), COOLDOWN[1])
            self.cool_until[model] = time.time() + secs
            self.cv.notify_all()
            return secs


def router_status():
    """{model_id: execution_status} from the router, or {} if it can't be read."""
    try:
        return {m["id"]: m.get("execution_status") or ("ready" if m.get("available", True) else "off")
                for m in http("GET", "/v1/models", timeout=20).get("data", [])}
    except Exception as e:
        print(f"(could not read router model list: {e}; using the pool as given)", file=sys.stderr)
        return {}


def make_pool(spec, pool_arg=None, per_model=2):
    if pool_arg:
        wanted = [m.strip() for m in pool_arg.split(",") if m.strip()]
    else:
        wanted = ([spec["model"]] if spec.get("model") else []) + \
                 (spec.get("model_pool") or DEFAULT_POOL) + spec.get("fallback_models", [])
    status = router_status()
    if status:  # keep models the router knows and can serve (exhausted ones start cooled down)
        usable = [m for m in dict.fromkeys(wanted) if status.get(m) in ("ready", "exhausted")]
    else:
        usable = list(dict.fromkeys(wanted))
    if not usable:
        usable = list(dict.fromkeys(wanted)) or ["auto"]
    cooling = [m for m in usable if status.get(m) == "exhausted"]
    return ModelPool(usable, per_model=per_model, cooling=cooling)


class Syncer:
    """Pushes one run folder to the dashboard server in the background."""

    def __init__(self, run_dir, enabled=True, background=False):
        self.run_dir = run_dir
        self.enabled = enabled and SYNC_DEST.lower() not in ("", "off", "0", "no") and shutil.which("rsync")
        self.dirty, self.stop = threading.Event(), threading.Event()
        self.warned, self.thread = False, None
        if self.enabled and background:
            self.thread = threading.Thread(target=self._loop, daemon=True)
            self.thread.start()

    def poke(self):
        self.dirty.set()

    def push(self):
        if not self.enabled:
            return False
        cmd = ["rsync", "-az", "--chmod=D755,F644", "-e", SYNC_SSH + " -o BatchMode=yes -o ConnectTimeout=5",
               f"{self.run_dir}/", f"{SYNC_DEST}{self.run_dir.name}/"]
        try:
            p = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            ok = p.returncode == 0
            lines = p.stderr.strip().splitlines()
            err = ([l for l in lines if "denied" in l.lower() or "refused" in l.lower() or "timed out" in l.lower()]
                   or lines[-1:]) if not ok else []
        except Exception as e:
            ok, err = False, [str(e)]
        if not ok and not self.warned:
            self.warned = True
            print(f"(dashboard sync failed: {' '.join(err)[:200]}; the run continues locally. "
                  f"Check with: swarm.py sync {self.run_dir.name})", file=sys.stderr)
        elif ok and self.warned:
            self.warned = False
            print("(dashboard sync working again)", file=sys.stderr)
        return ok

    def _loop(self):
        while not self.stop.is_set():
            self.dirty.wait(timeout=SYNC_EVERY)
            if self.dirty.is_set():
                self.dirty.clear()
                self.push()
            self.stop.wait(SYNC_EVERY)

    def close(self):
        if self.thread:
            self.stop.set()
            self.dirty.set()
            self.thread.join(timeout=5)
            self.push()


_syncers = {}


def start_sync(run_dir, enabled=True):
    sy = _syncers[run_dir.name] = Syncer(run_dir, enabled, background=True)
    return sy


def owned(path, globs):
    return not globs or any(fnmatch.fnmatch(path, g) for g in globs)


def safe_rel(p):
    p = p.strip()
    while p.startswith("./"):
        p = p[2:]  # (lstrip("./") also ate the dot of dotfiles like .gitignore)
    if not p or p.startswith("/") or ".." in Path(p).parts:
        return None
    return p


def build_messages(spec, task, feedback=None, previous=None, run_dir=None):
    project = Path(spec.get("project", "."))
    deps = task.get("depends_on") or task.get("after") or []
    parts = []
    if spec.get("shared_context"):
        parts.append(f"## Shared project context\n{spec['shared_context']}")
    parts.append(f"## Your task ({task['id']})\n{task['instructions']}")
    if task.get("owns"):
        parts.append("## Files you may create/modify (globs)\n" + "\n".join(task["owns"]))
    ctx = []
    for f in task.get("context_files", []):
        # Prefer a file a dependency just staged in this run over the project copy.
        staged = [run_dir / d / "files" / f for d in deps] if run_dir else []
        fp = next((p for p in staged if p.is_file()), project / f)
        if fp.is_file():
            ctx.append(f'<context path="{f}">\n{fp.read_text(errors="replace")}\n</context>')
        else:
            ctx.append(f'<context path="{f}">(file does not exist yet)</context>')
    if ctx:
        parts.append("## Existing files for reference\n" + "\n".join(ctx))
    dep_notes = []
    for d in deps:
        rj = run_dir / d / "result.json" if run_dir else None
        if rj and rj.is_file():
            r = json.loads(rj.read_text())
            dep_notes.append(f"### {d}\nfiles: {', '.join(r.get('files', []))}\n{r.get('notes', '')}")
    if dep_notes:
        parts.append("## Finished tasks you build on\n" + "\n\n".join(dep_notes))
    msgs = [{"role": "system", "content": task.get("system") or SYSTEM},
            {"role": "user", "content": "\n\n".join(parts)}]
    if previous is not None:
        msgs.append({"role": "assistant", "content": previous})
        msgs.append({"role": "user", "content":
                     "Supervisor review - your submission was NOT accepted. Fix every point below and "
                     "resubmit ALL affected files in full using the same format.\n\n" + feedback})
    return msgs


def parse_files(content, task, tdir):
    written, rejected = [], []
    for path, body in FILE_RE.findall(content):
        rel = safe_rel(path)
        if not rel or not owned(rel, task.get("owns")):
            rejected.append(path)
            continue
        out = tdir / "files" / rel
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(body if body.endswith("\n") else body + "\n")
        written.append(rel)
    return written, rejected


def run_task(spec, task, run_dir, pool, feedback=None, max_attempts=None):
    tid = task["id"]
    tdir = run_dir / tid
    previous = None
    if feedback:
        previous = (tdir / "response.md").read_text()
        rev = len(list(tdir.glob("response.r*.md"))) + 1
        shutil.copy(tdir / "response.md", tdir / f"response.r{rev}.md")
        emit(run_dir, "revise_start", tid, feedback=feedback, revision=rev)
    tdir.mkdir(parents=True, exist_ok=True)
    emit(run_dir, "task_start", tid, title=task.get("title", tid))
    msgs = build_messages(spec, task, feedback=feedback, previous=previous, run_dir=run_dir)
    result = {"id": tid, "title": task.get("title", tid), "ok": False, "errors": []}
    pinned = task.get("model")
    if pinned and pinned not in pool.busy:  # pinned model outside the pool: give it its own slot
        with pool.cv:
            pool.busy[pinned] = 0
    # avoid_models: e.g. a reviewer must not be the model that wrote the code.
    avoid = set(task.get("avoid_models") or [])
    if avoid and not set(pool.models) - avoid:
        avoid = set()  # nothing else available: better a same-model review than none
    skip, limited, attempts, ok, m = set(avoid), {}, 0, False, None
    max_attempts = max_attempts or 2 * len(pool.busy) + 2
    while attempts < max_attempts:
        m = pool.acquire(skip, prefer=pinned)
        if m is None:
            break
        attempts += 1
        emit(run_dir, "attempt", tid, model=m)
        ok = False
        try:
            content, used, secs, usage = chat(
                m, msgs, tdir / "live.md",
                lambda n, mdl: emit(run_dir, "progress", tid, chars=n, model=mdl))
            if (tdir / "files").exists():
                shutil.rmtree(tdir / "files")
            written, rejected = parse_files(content, task, tdir)
            if not written:
                raise RuntimeError("worker returned no parseable <file> blocks")
            ok = True
        except RateLimited as e:
            secs_cool = pool.cool(m)
            limited[m] = limited.get(m, 0) + 1
            if limited[m] >= 2:  # don't sit out a second cooldown for the same model
                skip.add(m)
            result["errors"].append(f"{m}: {e}")
            emit(run_dir, "attempt_error", tid, model=m, error=f"rate limited, resting {secs_cool}s: {str(e)[:300]}")
        except RuntimeError as e:
            skip.add(m)
            result["errors"].append(f"{m}: {e}")
            emit(run_dir, "attempt_error", tid, model=m, error=str(e)[:400])
        finally:
            pool.release(m, ok)
        if ok:
            break
    if not ok:
        (tdir / "result.json").write_text(json.dumps(result, indent=2))
        emit(run_dir, "task_failed", tid, errors=result["errors"])
        return result

    (tdir / "response.md").write_text(content)
    notes = NOTES_RE.search(content)
    result.update(ok=True, model=used, pool_model=m, seconds=secs, usage=usage, files=written,
                  rejected_paths=rejected, notes=notes.group(1).strip() if notes else "",
                  revision=len(list(tdir.glob("response.r*.md"))))
    (tdir / "result.json").write_text(json.dumps(result, indent=2))
    emit(run_dir, "task_done", tid, model=used, seconds=secs, usage=usage, files=written,
         rejected=rejected, notes=result["notes"], errors=result["errors"], revision=result["revision"])
    return result


def run_graph(spec, tasks, run_dir, pool, jobs):
    """Run tasks with up to `jobs` at once, starting each as soon as its depends_on are done."""
    ids = {t["id"] for t in tasks}
    deps = {t["id"]: [d for d in (t.get("depends_on") or t.get("after") or []) if d in ids] for t in tasks}
    pending = {t["id"]: t for t in tasks}
    done, failed, results, running = set(), set(), [], {}
    with cf.ThreadPoolExecutor(max(1, jobs)) as ex:
        while pending or running:
            for tid in [t for t in pending if any(d in failed for d in deps[t])]:
                bad = [d for d in deps[tid] if d in failed]
                t = pending.pop(tid)
                r = {"id": tid, "title": t.get("title", tid), "ok": False,
                     "errors": [f"skipped: depends on failed task(s) {', '.join(bad)}"]}
                (run_dir / tid).mkdir(parents=True, exist_ok=True)
                (run_dir / tid / "result.json").write_text(json.dumps(r, indent=2))
                emit(run_dir, "task_failed", tid, errors=r["errors"])
                failed.add(tid); results.append(r); print_result(r)
            for tid in [t for t in pending if all(d in done for d in deps[t])]:
                running[ex.submit(run_task, spec, pending.pop(tid), run_dir, pool)] = tid
            if not running:
                if pending:  # a cycle or unknown dependency
                    for tid, t in pending.items():
                        r = {"id": tid, "ok": False, "errors": ["dependency cycle"]}
                        emit(run_dir, "task_failed", tid, errors=r["errors"]); results.append(r); print_result(r)
                break
            finished, _ = cf.wait(running, return_when=cf.FIRST_COMPLETED)
            for fut in finished:
                tid = running.pop(fut)
                try:
                    r = fut.result()
                except Exception as e:  # never let one task kill the run
                    r = {"id": tid, "ok": False, "errors": [f"{type(e).__name__}: {e}"]}
                    emit(run_dir, "task_failed", tid, errors=r["errors"])
                (done if r["ok"] else failed).add(tid)
                results.append(r); print_result(r); sys.stdout.flush()
    return results


def print_result(r):
    mark = "OK  " if r["ok"] else "FAIL"
    print(f"[{mark}] {r['id']:<18} model={r.get('model','-')} t={r.get('seconds','-')}s "
          f"files={len(r.get('files', []))} rev={r.get('revision', 0)}")
    for f in r.get("files", []):
        print(f"         + {f}")
    for p in r.get("rejected_paths", []):
        print(f"         ! rejected (not owned / unsafe): {p}")
    if not r["ok"]:
        for e in r.get("errors", []):
            print(f"         ! {e[:200]}")
    if r.get("notes"):
        print("         notes: " + r["notes"].replace("\n", "\n                "))


def load_run(run):
    run_dir = SWARM_DIR / run
    if not run_dir.exists():
        sys.exit(f"no such run: {run_dir}")
    return run_dir, json.loads((run_dir / "tasks.json").read_text())


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--no-sync", action="store_true", help="don't push run data to the dashboard server")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("models"); p.add_argument("--all", action="store_true", help="include unusable models")
    p = sub.add_parser("run"); p.add_argument("tasks"); p.add_argument("-j", type=int, default=8,
                                                                      help="max tasks at once (default 8)")
    p.add_argument("--only", nargs="*", help="run only these task ids")
    p.add_argument("--pool", help="comma-separated models to spread tasks over (overrides the tasks file)")
    p.add_argument("--per-model", type=int, default=2, help="max tasks at once on one model (default 2)")
    p = sub.add_parser("revise"); p.add_argument("run"); p.add_argument("task"); p.add_argument("feedback")
    p.add_argument("--model", help="switch this task to a different model")
    p.add_argument("--pool"); p.add_argument("--per-model", type=int, default=2)
    p = sub.add_parser("revise-many", help="revise several tasks in parallel")
    p.add_argument("run"); p.add_argument("feedback_file",
                                          help='JSON {"task": "feedback"} or [{"task","feedback","model"?}]')
    p.add_argument("-j", type=int, default=8); p.add_argument("--pool"); p.add_argument("--per-model", type=int, default=2)
    p = sub.add_parser("note"); p.add_argument("run"); p.add_argument("args", nargs="+")
    p = sub.add_parser("verdict"); p.add_argument("run"); p.add_argument("task")
    p.add_argument("verdict", choices=["approved", "rejected"]); p.add_argument("why", nargs="?", default="")
    p = sub.add_parser("status"); p.add_argument("run")
    p = sub.add_parser("apply"); p.add_argument("run"); p.add_argument("task"); p.add_argument("--dest")
    p = sub.add_parser("sync", help="push run data to the dashboard server now")
    p.add_argument("run", nargs="?", help="one run (default: every run in SWARM_DIR)")
    a = ap.parse_args()
    sync = not a.no_sync

    if a.cmd == "models":
        for m in http("GET", "/v1/models").get("data", []):
            st = m.get("execution_status") or ("ready" if m.get("available", True) else "off")
            if a.all or st in ("ready", "exhausted"):
                print(f"{m.get('id'):<40} {st:<10} ctx={m.get('context_window')}")
    elif a.cmd == "run":
        spec = json.loads(Path(a.tasks).read_text())
        tasks = [t for t in spec["tasks"] if not a.only or t["id"] in a.only]
        run = datetime.now().strftime("%Y%m%d-%H%M%S")
        run_dir = SWARM_DIR / run
        run_dir.mkdir(parents=True)
        (run_dir / "tasks.json").write_text(json.dumps(spec, indent=2))
        pool = make_pool(spec, a.pool, a.per_model)
        syncer = start_sync(run_dir, sync)
        emit(run_dir, "run_start", name=spec.get("name", run),
             tasks=[{"id": t["id"], "title": t.get("title", t["id"])} for t in tasks])
        emit(run_dir, "supervisor_note", None,
             text=f"Model pool ({a.per_model} per model): {', '.join(pool.models)}")
        print(f"run {run}: {len(tasks)} tasks, up to {min(a.j, len(tasks))} at once over "
              f"{len(pool.models)} models: {', '.join(pool.models)}")
        if syncer.enabled:
            print(f"live on the dashboard: {DASHBOARD_URL}  (run {run})")
        for t in tasks:
            emit(run_dir, "task_queued", t["id"], title=t.get("title", t["id"]))
        try:
            run_graph(spec, tasks, run_dir, pool, a.j)
        finally:
            emit(run_dir, "run_end")
            syncer.close()
        print(f"\nstaged in {run_dir}/<task>/files - review, then `revise`, `revise-many` or `apply`.")
    elif a.cmd in ("revise", "revise-many"):
        run_dir, spec = load_run(a.run)
        if a.cmd == "revise":
            items = [{"task": a.task, "feedback": a.feedback, "model": a.model}]
        else:
            raw = json.loads(Path(a.feedback_file).read_text())
            items = [{"task": k, "feedback": v} for k, v in raw.items()] if isinstance(raw, dict) else raw
        by_id = {t["id"]: t for t in spec["tasks"]}
        missing = [i["task"] for i in items if i["task"] not in by_id]
        if missing:
            sys.exit(f"unknown task id(s): {', '.join(missing)}")
        pool = make_pool(spec, a.pool, a.per_model)
        syncer = start_sync(run_dir, sync)
        try:
            def one(item):
                task = dict(by_id[item["task"]])
                if item.get("model"):
                    task["model"] = item["model"]
                emit(run_dir, "supervisor_note", task["id"], text=f"Sent back for revision: {item['feedback']}")
                return run_task(spec, task, run_dir, pool, feedback=item["feedback"])
            with cf.ThreadPoolExecutor(max(1, getattr(a, "j", 1))) as ex:
                for fut in cf.as_completed([ex.submit(one, i) for i in items]):
                    print_result(fut.result()); sys.stdout.flush()
        finally:
            syncer.close()
    elif a.cmd == "sync":
        runs = [SWARM_DIR / a.run] if a.run else sorted(p for p in SWARM_DIR.iterdir() if (p / "events.jsonl").is_file())
        bad = 0
        for rd in runs:
            ok = Syncer(rd).push() if rd.is_dir() else False
            bad += not ok
            print(f"{'synced' if ok else 'FAILED'} {rd.name}")
        print(f"dashboard: {DASHBOARD_URL}")
        sys.exit(1 if bad else 0)
    elif a.cmd == "note":
        run_dir, _ = load_run(a.run)
        task, text = (a.args[0], " ".join(a.args[1:])) if len(a.args) > 1 else (None, a.args[0])
        emit(run_dir, "supervisor_note", task, text=text)
        Syncer(run_dir, sync).push()
    elif a.cmd == "verdict":
        run_dir, _ = load_run(a.run)
        emit(run_dir, "verdict", a.task, verdict=a.verdict, text=a.why)
        Syncer(run_dir, sync).push()
    elif a.cmd == "status":
        run_dir, _ = load_run(a.run)
        for rj in sorted(run_dir.glob("*/result.json")):
            print_result(json.loads(rj.read_text()))
    elif a.cmd == "apply":
        run_dir, spec = load_run(a.run)
        src = run_dir / a.task / "files"
        dest = Path(a.dest or spec.get("project", "."))
        applied = []
        for f in sorted(p for p in src.rglob("*") if p.is_file()):
            rel = f.relative_to(src)
            (dest / rel).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(f, dest / rel)
            applied.append(str(rel))
            print(f"applied {dest / rel}")
        emit(run_dir, "apply", a.task, files=applied)
        Syncer(run_dir, sync).push()


if __name__ == "__main__":
    main()
