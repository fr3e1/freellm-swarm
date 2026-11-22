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
                    │  :8787           │
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
# 1. Configure FreeLLM API
export FREELLMAPI_URL="https://your-freellm-api.example.com"
export FREELLMAPI_KEY="sk-..."   # or put key in ~/.config/freellmapi/key

# 2. Write a tasks.json (see format below)
cat > tasks.json <<'EOF'
{
  "project": "hello-world",
  "tasks": [
    { "id": "write-main", "prompt": "Write a Python CLI that prints 'Hello, Swarm!'" },
    { "id": "write-test", "prompt": "Write a pytest for the CLI", "depends_on": ["write-main"] }
  ]
}
EOF

# 3. Run the swarm
python swarm.py tasks.json

# 4. (Optional) Watch live in another terminal
SWARM_DIR=.swarm python -m dashboard.server
# Open http://localhost:8787/
```

---

## `swarm.py` Command Reference

```bash
python swarm.py <tasks.json> [options]
```

| Option | Description |
|--------|-------------|
| `--run-dir DIR` | Override `.swarm/<timestamp>` run directory |
| `--max-parallel N` | Max concurrent workers (default: 4) |
| `--model MODEL` | Override model for all tasks |
| `--temperature FLOAT` | Sampling temperature (default: 0.2) |
| `--max-tokens N` | Max tokens per completion (default: 4096) |
| `--timeout SECS` | HTTP timeout per request (default: 120) |
| `--no-stream` | Disable streaming, wait for full response |
| `-v, --verbose` | Verbose logging to stderr |

**Environment variables** (read by `swarm.py`):
- `FREELLMAPI_URL` — **required**, base URL of FreeLLM API (e.g. `https://api.example.com`)
- `FREELLMAPI_KEY` — API key, or use `~/.config/freellmapi/key`

---

## `tasks.json` Format

```json
{
  "project": "my-project",
  "model": "default-model-name",           // optional, per-task override allowed
  "temperature": 0.2,                       // optional
  "max_tokens": 4096,                       // optional
  "tasks": [
    {
      "id": "unique-task-id",               // required, ^[A-Za-z0-9_.-]+$
      "prompt": "Task instruction...",      // required
      "depends_on": ["other-task-id"],      // optional, DAG dependencies
      "model": "task-specific-model",       // optional override
      "temperature": 0.5,                   // optional override
      "max_tokens": 2048                    // optional override
    }
  ]
}
```

- Tasks run in parallel respecting `depends_on` (topological order).
- Each task gets a directory `.swarm/<run>/tasks/<id>/` containing:
  - `live.md` — streaming worker output (updated in real time)
  - Staged files written by `apply` (relative paths preserved)

---

## Dashboard — Local Run

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
