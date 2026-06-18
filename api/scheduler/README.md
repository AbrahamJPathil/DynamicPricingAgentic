# Agent Scheduler

Runs every agent listed in `agents_registry.py` automatically on the
clock hour (1:00, 2:00, 3:00, ...), and exposes an API to force an
immediate rerun of a single agent on demand.

## Setup

```bash
pip install -r requirements.txt
```

Place this script alongside your existing `agents/` folder (it expects
`agents/inventory/agent.py`, `agents/competitor/agent.py`, etc. — see
`agents_registry.py` to add more).

## Run

```bash
AGENT_API_KEY=your-secret-here uvicorn main:app --host 0.0.0.0 --port 8000
```

Set `AGENT_API_KEY` to something real — it's the key required on the
rerun endpoint. If you don't set it, "change-me" is used, which is
fine for local testing but not for anything exposed beyond your own
machine.

## Endpoints

- `GET /agents` — lists known agents and when each is next due on its
  hourly schedule.
- `POST /agents/{agent_name}/rerun` — forces an immediate one-off run
  of that agent right now, without changing its hourly schedule.
  Requires header `X-API-Key: your-secret-here`.

```bash
curl -X POST http://localhost:8000/agents/inventory/rerun \
     -H "X-API-Key: your-secret-here"
```

Responses:
- `200` — run has been scheduled to start immediately
- `404` — unknown agent name
- `401` — missing/wrong API key
- `409` — that agent is already running (scheduled or manual); try
  again once it finishes

## How it works

- `agents_registry.py` — the single place mapping agent name → script
  path. Add a new agent here and nothing else needs to change.
- `runner.py` — runs an agent's `agent.py` as an isolated subprocess
  (so a crash in one agent can't take down the scheduler), logs each
  run to `logs/<agent>.log`, and holds a per-agent lock so a scheduled
  run and a manual rerun of the same agent can never overlap.
- `main.py` — registers one `CronTrigger(minute=0)` job per agent
  (hourly, on the clock) and exposes the FastAPI rerun/list endpoints.

## Notes / things you may want to tune

- `AGENT_TIMEOUT_SECONDS` in `runner.py` (currently 30 min) — how long
  a single agent run is allowed to run before being killed.
- If an agent is busy when its hourly slot comes around, the
  scheduled job simply skips that run (via `max_instances=1`,
  `coalesce=True`) rather than queuing up backlogged runs.
- Logs are plain text files under `logs/`; swap in structured logging
  or a log aggregator if you need more than that.
