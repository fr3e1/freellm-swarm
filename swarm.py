#!/usr/bin/env python3
"""swarm.py - farm development tasks out to FreeLLMAPI workers in parallel.

Claude (supervisor) writes a tasks file, runs the workers, reviews the staged
output, sends revisions back, and only then applies files to the project.

  swarm.py models                         list models the router offers
  swarm.py run tasks.json [-j N]          run all tasks in parallel -> .swarm/<run>/
  swarm.py revise <run> <task> "feedback" re-run one task with review feedback
  swarm.py note <run> [task] "text"       log a supervisor note (shown on the dashboard)
  swarm.py verdict <run> <task> approved|rejected ["why"]
  swarm.py status <run>                   summary of a run
  swarm.py apply <run> <task> [--dest D]  copy a task's approved files into the project

Every run writes .swarm/<run>/events.jsonl (one JSON event per line) and each
task streams its in-progress output to .swarm/<run>/<task>/live.md; the
dashboard reads both.

tasks.json:
{
  "name": "human readable run name",
  "project": "path/to/project",          # files are read from / applied to here
  "model": "auto",                       # default model (overridable per task)
  "fallback_models": ["..."],            # tried in order if the main model errors
  "shared_context": "Stack, conventions, interfaces every worker must follow",
  "tasks": [
    {"id": "api", "title": "short label", "instructions": "...", "context_files": ["src/types.ts"],
     "owns": ["src/api/**"], "model": "optional-override"}
  ]
}

Config: FREELLMAPI_URL (default http://192.168.10.122:3001) and FREELLMAPI_KEY,
or the key on one line in ~/.config/freellmapi/key. SWARM_DIR overrides .swarm.
"""
import argparse, concurrent.futures as cf, fnmatch, json, os, re, shutil, sys, threading, time
import urllib.request, urllib.error
from datetime import datetime
from pathlib import Path

BASE = os.environ.get("FREELLMAPI_URL", "http://192.168.10.122:3001").rstrip("/")
KEY_FILE = Path.home() / ".config/freellmapi/key"
SWARM_DIR = Path(os.environ.get("SWARM_DIR", ".swarm"))

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
FILE_RE = re.compile(r'<file\s+path=["\']?([^"\'>\n]+?)["\']?\s*>\n?(.*?)\n?(?:</file>|(?=<file\s+path=)|(?=<notes>)|\Z)', re.S)
NOTES_RE = re.compile(r"<notes>(.*?)</notes>", re.S)
_lock = threading.Lock()


def emit(run_dir, type_, task=None, **data):
    ev = {"ts": round(time.time(), 3), "run": run_dir.name, "task": task, "type": type_, **data}
    with _lock, open(run_dir / "events.jsonl", "a") as f:
        f.write(json.dumps(ev) + "\n")


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
    text = "".join(buf)
    live_path.write_text(text)
    if not text.strip():
        raise RuntimeError("empty response")
    return text, used, round(time.time() - t, 1), usage


def chat(model, messages, live_path, on_progress, retries=3):
    last = None
    for attempt in range(retries):
        try:
            return chat_stream(model, messages, live_path, on_progress)
        except urllib.error.HTTPError as e:
            last = f"HTTP {e.code}: {e.read().decode(errors='replace')[:300]}"
            if e.code in (400, 401, 403, 404, 429):  # 429 here = router already exhausted; try next model
                break
        except Exception as e:  # timeouts, bad json, network
            last = f"{type(e).__name__}: {e}"
        time.sleep(2 * (attempt + 1))
    raise RuntimeError(last)


def owned(path, globs):
    return not globs or any(fnmatch.fnmatch(path, g) for g in globs)


def safe_rel(p):
    p = p.strip().lstrip("./")
    if not p or p.startswith("/") or ".." in Path(p).parts:
        return None
    return p


def build_messages(spec, task, feedback=None, previous=None):
    project = Path(spec.get("project", "."))
    parts = []
    if spec.get("shared_context"):
        parts.append(f"## Shared project context\n{spec['shared_context']}")
    parts.append(f"## Your task ({task['id']})\n{task['instructions']}")
    if task.get("owns"):
        parts.append("## Files you may create/modify (globs)\n" + "\n".join(task["owns"]))
    ctx = []
    for f in task.get("context_files", []):
        fp = project / f
        if fp.is_file():
            ctx.append(f'<context path="{f}">\n{fp.read_text(errors="replace")}\n</context>')
        else:
            ctx.append(f'<context path="{f}">(file does not exist yet)</context>')
    if ctx:
        parts.append("## Existing files for reference\n" + "\n".join(ctx))
    msgs = [{"role": "system", "content": SYSTEM}, {"role": "user", "content": "\n\n".join(parts)}]
    if previous is not None:
        msgs.append({"role": "assistant", "content": previous})
        msgs.append({"role": "user", "content":
                     "Supervisor review - your submission was NOT accepted. Fix every point below and "
                     "resubmit ALL affected files in full using the same format.\n\n" + feedback})
    return msgs


def run_task(spec, task, run_dir, feedback=None):
    tid = task["id"]
    tdir = run_dir / tid
    previous = None
    if feedback:
        previous = (tdir / "response.md").read_text()
        rev = len(list(tdir.glob("response.r*.md"))) + 1
        shutil.copy(tdir / "response.md", tdir / f"response.r{rev}.md")
        if (tdir / "files").exists():
            shutil.rmtree(tdir / "files")
        emit(run_dir, "revise_start", tid, feedback=feedback, revision=rev)
    tdir.mkdir(parents=True, exist_ok=True)
    emit(run_dir, "task_start", tid, title=task.get("title", tid))
    msgs = build_messages(spec, task, feedback=feedback, previous=previous)
    models = [task.get("model") or spec.get("model", "auto")] + spec.get("fallback_models", [])
    result = {"id": tid, "title": task.get("title", tid), "ok": False, "errors": []}
    for m in models:
        emit(run_dir, "attempt", tid, model=m)
        try:
            content, used, secs, usage = chat(
                m, msgs, tdir / "live.md",
                lambda n, mdl: emit(run_dir, "progress", tid, chars=n, model=mdl))
            result.update(model=used, seconds=secs, usage=usage)
            break
        except RuntimeError as e:
            result["errors"].append(f"{m}: {e}")
            emit(run_dir, "attempt_error", tid, model=m, error=str(e)[:400])
    else:
        (tdir / "result.json").write_text(json.dumps(result, indent=2))
        emit(run_dir, "task_failed", tid, errors=result["errors"])
        return result

    (tdir / "response.md").write_text(content)
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
    notes = NOTES_RE.search(content)
    result.update(ok=bool(written), files=written, rejected_paths=rejected,
                  notes=notes.group(1).strip() if notes else "",
                  revision=len(list(tdir.glob("response.r*.md"))))
    if not written:
        result["errors"].append("worker returned no parseable <file> blocks")
    (tdir / "result.json").write_text(json.dumps(result, indent=2))
    emit(run_dir, "task_done" if result["ok"] else "task_failed", tid,
         model=result.get("model"), seconds=result.get("seconds"), usage=result.get("usage"),
         files=written, rejected=rejected, notes=result["notes"], errors=result["errors"],
         revision=result["revision"])
    return result


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
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("models")
    p = sub.add_parser("run"); p.add_argument("tasks"); p.add_argument("-j", type=int, default=6)
    p.add_argument("--only", nargs="*", help="run only these task ids")
    p = sub.add_parser("revise"); p.add_argument("run"); p.add_argument("task"); p.add_argument("feedback")
    p.add_argument("--model", help="switch this task to a different model")
    p = sub.add_parser("note"); p.add_argument("run"); p.add_argument("args", nargs="+")
    p = sub.add_parser("verdict"); p.add_argument("run"); p.add_argument("task")
    p.add_argument("verdict", choices=["approved", "rejected"]); p.add_argument("why", nargs="?", default="")
    p = sub.add_parser("status"); p.add_argument("run")
    p = sub.add_parser("apply"); p.add_argument("run"); p.add_argument("task"); p.add_argument("--dest")
    a = ap.parse_args()

    if a.cmd == "models":
        for m in http("GET", "/v1/models").get("data", []):
            if m.get("available", True):
                print(m.get("id"), " ctx=", m.get("context_window"))
    elif a.cmd == "run":
        spec = json.loads(Path(a.tasks).read_text())
        tasks = [t for t in spec["tasks"] if not a.only or t["id"] in a.only]
        run = datetime.now().strftime("%Y%m%d-%H%M%S")
        run_dir = SWARM_DIR / run
        run_dir.mkdir(parents=True)
        (run_dir / "tasks.json").write_text(json.dumps(spec, indent=2))
        emit(run_dir, "run_start", name=spec.get("name", run),
             tasks=[{"id": t["id"], "title": t.get("title", t["id"])} for t in tasks])
        print(f"run {run}: {len(tasks)} tasks, {min(a.j, len(tasks))} in parallel")
        with cf.ThreadPoolExecutor(a.j) as ex:
            for t in tasks:
                emit(run_dir, "task_queued", t["id"], title=t.get("title", t["id"]))
            for fut in cf.as_completed([ex.submit(run_task, spec, t, run_dir) for t in tasks]):
                print_result(fut.result()); sys.stdout.flush()
        emit(run_dir, "run_end")
        print(f"\nstaged in {run_dir}/<task>/files - review, then `revise` or `apply`.")
    elif a.cmd == "revise":
        run_dir, spec = load_run(a.run)
        task = next(t for t in spec["tasks"] if t["id"] == a.task)
        if a.model:
            task["model"] = a.model
        emit(run_dir, "supervisor_note", a.task, text=f"Sent back for revision: {a.feedback}")
        print_result(run_task(spec, task, run_dir, feedback=a.feedback))
    elif a.cmd == "note":
        run_dir, _ = load_run(a.run)
        task, text = (a.args[0], " ".join(a.args[1:])) if len(a.args) > 1 else (None, a.args[0])
        emit(run_dir, "supervisor_note", task, text=text)
    elif a.cmd == "verdict":
        run_dir, _ = load_run(a.run)
        emit(run_dir, "verdict", a.task, verdict=a.verdict, text=a.why)
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


if __name__ == "__main__":
    main()
