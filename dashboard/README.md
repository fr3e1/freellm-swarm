# Swarm Live Dashboard

A live web dashboard for watching AI worker agents (run by `swarm.py`) build software while a supervisor (Claude) reviews them.

The dashboard is a **read-only** viewer over the run data produced by `swarm.py`. It never modifies the swarm directory.

## Running locally

From the **project root**:

```bash
python -m dashboard.server
```

Environment variables:

| Variable | Default | Description |
|---|---|---|
| `SWARM_DIR` | `.swarm` | Directory containing run subdirectories (read-only). |
| `HOST` | `0.0.0.0` | Bind address. |
| `PORT` | `8787` | TCP port. |
| `DASHBOARD_TOKEN` | (unset) | Optional bearer token. When set, all `/api/*` requests require `?token=<t>` or `Authorization: Bearer <t>`. Static files stay public. |

Example:

```bash
SWARM_DIR=/data/swarm PORT=8787 DASHBOARD_TOKEN=changeme python -m dashboard.server
```

Then open `http://localhost:8787/`.

## API (brief)

All JSON responses are UTF-8 with `Cache-Control: no-store`.

- `GET /` — dashboard UI (`dashboard/static/index.html`)
- `GET /static/<path>` — static assets (public)
- `GET /healthz` — unauthenticated health check, returns `{"ok": true}`
- `GET /api/runs` — run summaries, newest first
- `GET /api/runs/<run>` — full run state: tasks + last 300 events
- `GET /api/runs/<run>/tasks/<task>/live` — `live.md` text (optional `?tail=N`)
- `GET /api/runs/<run>/tasks/<task>/files` — staged file list
- `GET /api/runs/<run>/tasks/<task>/file?path=<rel>` — staged file content
- `GET /api/stats` — aggregated token/task/model stats
- `GET /api/events` — Server-Sent Events stream of new events

Run/task ids must match `^[A-Za-z0-9_.-]+$`; unknown routes return 404 JSON.

## Security headers

Every HTTP response from the dashboard server includes:

- `X-Content-Type-Options: nosniff`
- `Referrer-Policy: no-referrer`
- `X-Frame-Options: DENY`
- `Content-Security-Policy: default-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'`

These headers are added by the server itself. When running behind nginx, the example config also adds them (the `always` flag ensures they appear on error responses too).

## SSE connection limit

The `/api/events` endpoint (Server-Sent Events) accepts at most **32 concurrent connections** per server process (configurable via the `sse_limit` attribute on the server). The 33rd simultaneous connection receives a `503` JSON response. This limit protects the server from resource exhaustion.

## Security note

The dashboard exposes worker output, staged files, and supervisor notes — effectively source code and internal logs produced by the AI workers. Treat it as sensitive:

- Always set `DASHBOARD_TOKEN` in production. **The token must be long and random** (e.g. generate with `python -c 'import secrets;print(secrets.token_urlsafe(32))'`).
- Terminate TLS (HTTPS) in front of the dashboard (e.g. nginx) so the token and payloads are encrypted.
- Do not expose the dashboard port directly to the public internet without authentication and TLS.

## Deployment options

### 1. Docker

Build from the **project root** (the Docker build context is the project root, not `dashboard/`):

```bash
docker build -f dashboard/Dockerfile -t swarm-dashboard .
```

Run with the swarm directory mounted read-only at `/data/swarm`:

```bash
docker run -d --name swarm-dashboard \
  -p 8787:8787 \
  -v /host/path/to/swarm:/data/swarm:ro \
  -e SWARM_DIR=/data/swarm \
  -e HOST=0.0.0.0 \
  -e PORT=8787 \
  -e DASHBOARD_TOKEN=changeme \
  swarm-dashboard
```

The container runs as a non-root user and exposes port 8787. A `HEALTHCHECK` polls `http://127.0.0.1:8787/healthz` (works even when `DASHBOARD_TOKEN` is set).

### 2. systemd

Install `dashboard/swarm-dashboard.service` (e.g. to `/etc/systemd/system/swarm-dashboard.service`), adjust `WorkingDirectory`, `SWARM_DIR`, and `DASHBOARD_TOKEN`, then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now swarm-dashboard
```

The unit uses `DynamicUser=yes`, `NoNewPrivileges=yes`, `ProtectSystem=strict`, and mounts the swarm directory read-only via `ReadOnlyPaths`.

### 3. nginx reverse proxy

Copy `dashboard/nginx.conf.example` into your nginx configuration (e.g. `/etc/nginx/conf.d/swarm-dashboard.conf`), adjust `server_name` and the upstream port, enable TLS, and reload:

```bash
sudo nginx -t
sudo systemctl reload nginx
```

nginx proxies to the dashboard backend on `127.0.0.1:8787` with SSE-friendly settings (`proxy_buffering off`, `proxy_read_timeout 1h`, `X-Accel-Buffering`). The example config also:

- Uses a custom `log_format` that logs `$uri` (path only) instead of `$request`, so `?token=` query parameters never appear in access logs.
- Adds a `limit_req` zone for `/api/` endpoints (30 req/s per IP, burst 50).
- Includes HSTS (`Strict-Transport-Security`) on the TLS server block.

Put the dashboard behind TLS and combine with `DASHBOARD_TOKEN`.

## Project layout

```
dashboard/
  Dockerfile
  .dockerignore
  swarm-dashboard.service
  nginx.conf.example
  README.md
  server.py          # HTTP API + static file server
  static/
    index.html
    app.js
    style.css
```

Python 3.11+, standard library only on the server; vanilla HTML/CSS/JS on the client (no build step, no CDN, no frameworks).
