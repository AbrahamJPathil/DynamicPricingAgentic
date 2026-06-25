# Dynamic Pricing Agentic

A multi-agent system for automated retail grocery pricing. Specialized AI agents analyze inventory perishability, competitor prices, seasonal events, and social media trends — then an orchestrator agent synthesizes their recommendations into final price updates.

## Architecture

```
[Inventory Agent] ──► inventory-agent ──┐
[Competitor Agent] ─► competitor-agent ──┤
[Event Agent] ──────► event-agent ───────┤
                                         ▼
                              Pricing Orchestrator
                                         │
                                         ▼
                                    final-prices
                                         │
                                    Supabase / API
```

- **Agents**: LangGraph state-machine agents powered by Google Gemini, each producing recommendations to Apache Kafka topics.
- **Orchestrator**: Kafka consumer that debounces partial data (90s window), synthesizes a final decision via Gemini, and writes to `final-prices`.
- **API**: FastAPI dashboard with review queue, SKU pricing context, and KPI endpoints.
- **Scheduler**: APScheduler running all agents on the hour, with manual rerun support via API.

## Tech Stack

Python 3.13+, LangGraph, Google Gemini, Apache Kafka, FastAPI, Supabase, APScheduler, Pydantic, Docker.

## Getting Started

1. Configure `.env` with `GEMINI_API_KEY`, `SUPABASE_URL`, `SUPABASE_SERVICE_KEY`, `KROGER_CLIENT_ID`, `KROGER_CLIENT_SECRET`, and `AGENT_API_KEY`.
2. Start Kafka: `docker compose -f docker/kafka-compose.yaml up -d`
3. Create topics: `inventory-agent`, `competitor-agent`, `final-prices`
4. Install dependencies (see per-component `requirements*.txt`).
5. Run agents individually or via the scheduler: `uvicorn scheduler.main:app`

## Project Structure

| Path | Description |
|---|---|
| `agents/` | Individual pricing agents (inventory, competitor, event, social media) and orchestrator |
| `api/` | FastAPI dashboard and KPI routes |
| `scheduler/` | Hourly cron scheduler with agent registry |
| `data/` | Input CSVs, festival calendar, local SQLite DB |
| `docker/` | Kafka Docker Compose configuration |
