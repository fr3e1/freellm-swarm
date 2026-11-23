# FreeLLM Swarm

A parallel AI software development system: a **supervisor** (Claude Code) splits a project into tasks, runs **FreeLLM API worker models** in parallel via the OpenAI-compatible `/v1/chat/completions` endpoint, streams their output, reviews it, requests revisions, and applies approved files. A **read-only live dashboard** lets you watch runs in real time.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Supervisor (Claude Code)                                       │
│  • Parses tasks.json → creates task directories                 │
│  • Spawns workers in parallel via FreeLLM API                   │
│  • Streams worker output, reviews, sends `revise` / `note`      │
│  • On approval: `apply` stages files to task directory          │
│  • Logs every event to .swarm/<run>/events.jsonl                │
└─────────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
        ┌──────────┐    ┌──────────┐    ┌──────────┐
        │ Worker 1 │    │ Worker 2 │    │ Worker N │
        │ (model)  │    │ (model)  │    │ (model)  │
        └──────────┘    └──────────┘    └──────────┘
              │               │               │
              └───────────────┼───────────────┘
                              ▼
                    ┌──────────────────┐
                    │  .swarm/<run>/   │
                    │  events.jsonl    │
                    │  tasks/<task>/   │
                    │    live.md       │
                    │    staged files  │
                    └──────────────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │  Dashboard       │
                    │  (read-only)     │
                    │ 192.168.10.123   │
                    └──────────────────┘
```

- **Supervisor**: `swarm.py` — orchestrates the whole loop.
- **Workers**: Any model served by FreeLLM API (OpenAI-compatible `/v1/chat/completions`).
- **Dashboard**: `dashboard/server.py` — static file server + JSON API + SSE stream, zero dependencies (Python 3.11+ stdlib only).

---

## Requirements

- Python **3.11+**
- FreeLLM API endpoint (`FREELLMAPI_URL`) and API key (`FREELLMAPI_KEY` or `~/.config/freellmapi/key`)
- For dashboard: same Python, no extra packages

---

## Quick Start

```bash
# 1. FreeLLM API (defaults to the LAN router at http://192.168.10.122:3001)
export FREELLMAPI_KEY="sk-..."   # or put the key in ~/.config/freellmapi/key

# 2. Write a tasks.json (see format below), then run it
python swarm.py run tasks.json

# 3. Watch it live on the dashboard server
#    http://192.168.10.123:8787/?token=<DASHBOARD_TOKEN>
```

Every command that changes a run (`run`, `revise`, `revise-many`, `note`, `verdict`, `apply`) pushes that run folder to the dashboard server with rsync over SSH, every few seconds while workers stream. If the push fails, the run carries on locally and `swarm.py sync` retries it.

---

## `swarm.py` Command Reference

```text
swarm.py [--no-sync] models [--all]                  models the router can serve now
swarm.py [--no-sync] run tasks.json [-j N] [--pool a,b,c] [--per-model N] [--only id ...]
swarm.py [--no-sync] revise <run> <task> "feedback" [--model M]
swarm.py [--no-sync] revise-many <run> feedback.json [-j N]   # {"task": "feedback", ...}
swarm.py [--no-sync] note <run> [task] "text"
swarm.py [--no-sync] verdict <run> <task> approved|rejected ["why"]
swarm.py status <run>
swarm.py [--no-sync] apply <run> <task> [--dest DIR]
swarm.py sync [run]                                  push one run (or all) to the dashboard
```

**Parallelism.** Tasks are spread over a pool of models, at most `--per-model` (default 2) at a time on each, with up to `-j` (default 8) tasks at once. The pool is `--pool`, else the file's `model` + `model_pool` (or the built-in `DEFAULT_POOL`) + `fallback_models`, filtered to models the router reports as usable. A model that answers 429 rests (60 s, doubling up to 10 min) while its task moves to the next free model; a model that errors or returns no `<file>` blocks is dropped for that task. Tasks with `depends_on` start the moment those finish, and see their staged files and notes.

**Environment variables:**

| Variable | Default | Description |
|----------|---------|-------------|
| `FREELLMAPI_URL` | `http://192.168.10.122:3001` | FreeLLM API router |
| `FREELLMAPI_KEY` | `~/.config/freellmapi/key` | API key |
| `SWARM_DIR` | `.swarm` | Local run data |
| `SWARM_DASHBOARD_URL` | `http://192.168.10.123:8787/` | Dashboard link printed by `run` |
| `SWARM_SYNC` | `swarmsync@192.168.10.123:` | rsync target (`off` disables) |
| `SWARM_SYNC_SSH` | `ssh` | SSH command for rsync, e.g. `ssh -i ~/.ssh/id_ed25519` |
| `SWARM_COOLDOWN` | `60` | First rest after a 429, in seconds |

---

## `tasks.json` Format

```json
{
  "name": "Human readable run name",
  "project": "path/to/project",
  "model": "nemotron-3-ultra-550b",
  "model_pool": ["nemotron-3-ultra-550b", "kilo-auto", "gpt-oss-120b"],
  "fallback_models": ["auto"],
  "shared_context": "Stack, conventions and interfaces every worker must follow",
  "tasks": [
    {
      "id": "api",
      "title": "Short label",
      "instructions": "What to build",
      "owns": ["src/api/**"],
      "context_files": ["src/types.ts"],
      "depends_on": ["types"],
      "model": "optional: pin this task to one model first"
    }
  ]
}
```

- Each task stages its output in `.swarm/<run>/<task>/`: `live.md` (streaming output), `response.md`, `result.json` and `files/` (only paths matching `owns`).
- Keep tasks independent where you can: that's what lets them all run at once. Use `depends_on` only when a task really needs another's files.

---

## Dashboard server

The dashboard runs on CT 123 at **http://192.168.10.123:8787/** (LAN and VPN only), reading `/data/swarm`, which `swarm.py` fills over rsync as the `swarmsync` user.

### Running a dashboard locally

```bash
# From project root
SWARM_DIR=.swarm PORT=8787 DASHBOARD_TOKEN=$(python3 -c 'import secrets;print(secrets.token_urlsafe(32))') python -m dashboard.server
```

Open `http://localhost:8787/?token=<your-token>`.

**Environment variables:**

| Variable | Default | Description |
|----------|---------|-------------|
| `SWARM_DIR` | `.swarm` | Run data directory (read-only) |
| `HOST` | `0.0.0.0` | Bind address |
| `PORT` | `8787` | TCP port |
| `DASHBOARD_TOKEN` | (unset) | Bearer token for `/api/*`; required in production |

**API endpoints** (all JSON, `Cache-Control: no-store`):
- `GET /healthz` — unauthenticated health check
- `GET /api/runs` — run summaries, newest first
- `GET /api/runs/<run>` — full run state (tasks + last 300 events)
- `GET /api/runs/<run>/tasks/<task>/live` — `live.md` text (`?tail=N` optional)
- `GET /api/runs/<run>/tasks/<task>/files` — staged file list
- `GET /api/runs/<run>/tasks/<task>/file?path=<rel>` — staged file content
- `GET /api/stats` — aggregated token/task/model stats
- `GET /api/events` — SSE stream of new events (max 32 concurrent connections)

---

## Full Deployment Docs

See the **GitHub Pages site** for complete deployment instructions (Docker, systemd, nginx, TLS, hardening):

👉 **https://fr3e1.github.io/freellm-swarm/**

### Docker (summary)

```bash
# Build from project root (context = repo root)
docker build -f dashboard/Dockerfile -t swarm-dashboard .

# Run
docker run -d --name swarm-dashboard \
  -p 8787:8787 \
  -v /host/path/to/.swarm:/data/swarm:ro \
  -e SWARM_DIR=/data/swarm \
  -e HOST=0.0.0.0 \
  -e PORT=8787 \
  -e DASHBOARD_TOKEN=changeme \
  swarm-dashboard
```

### systemd (summary)

```bash
# Install unit (adjust WorkingDirectory, SWARM_DIR, DASHBOARD_TOKEN)
sudo cp dashboard/swarm-dashboard.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now swarm-dashboard
```

### nginx (summary)

```bash
sudo cp dashboard/nginx.conf.example /etc/nginx/conf.d/swarm-dashboard.conf
# Edit server_name, upstream port, TLS cert paths
sudo nginx -t && sudo systemctl reload nginx
```

---

## Security Notes

- **Dashboard exposes worker output, staged files, and supervisor notes** — effectively source code and internal logs. Treat as sensitive.
- **Always set `DASHBOARD_TOKEN` in production.** Generate a long random token:
  ```bash
  python3 -c 'import secrets;print(secrets.token_urlsafe(32))'
  ```
- **Terminate TLS (HTTPS) in front of the dashboard** (nginx) so the token and payloads are encrypted.
- **Never expose the dashboard port directly to the public internet** without authentication and TLS.
- The dashboard adds security headers on every response: `X-Content-Type-Options: nosniff`, `Referrer-Policy: no-referrer`, `X-Frame-Options: DENY`, and a restrictive `Content-Security-Policy`.
- nginx example config logs only `$uri` (path), never `$request`, so `?token=` never appears in access logs.
- Rate limiting: nginx example includes `limit_req` zone (30 req/s per IP, burst 50) on `/api/`.

---

## Repository Layout

```
freellm-swarm/
├── swarm.py                 # Orchestrator (supervisor)
├── tasks.json.example       # Example task definition
├── README.md                # This file
├── dashboard/
│   ├── Dockerfile
│   ├── .dockerignore
│   ├── swarm-dashboard.service
│   ├── nginx.conf.example
│   ├── README.md            # Dashboard-specific docs
│   ├── server.py            # HTTP API + static file server
│   └── static/
│       ├── index.html
│       ├── app.js
│       └── style.css
└── .swarm/                  # Created at runtime (gitignored)
    └── <run-id>/
        ├── events.jsonl     # Full event log
        └── tasks/
            └── <task-id>/
                ├── live.md
                └── ...staged files...
```

---

## License

MIT — see `LICENSE` (if present) or assume MIT for this project.
