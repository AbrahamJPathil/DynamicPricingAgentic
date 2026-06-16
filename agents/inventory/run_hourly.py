"""
run_hourly.py

Scheduler wrapper for agents/inventory/agent.py - no changes to agent.py
itself. This runs `python agent.py` as a subprocess once immediately, then
every hour on the hour (:00 UTC), forever, until interrupted.

Why a subprocess instead of importing agent.py:
  - agent.py is a script, not a library: it builds a fresh LangGraph,
    reads the API key from .env, and runs to completion inside main().
    Shelling out runs it exactly as if you'd typed
    `python agents/inventory/agent.py` yourself, with no risk of state
    leaking between hourly runs.
  - A crash inside agent.py (bad API key, malformed LLM output, etc.)
    is just a non-zero exit code here - it cannot take the scheduler
    process down, so the next hourly run still fires.

Each run's full stdout/stderr is captured to logs/run_<timestamp>.log,
and a one-line summary goes to the console/wrapper log.

Run (from the project root, same place you'd run agent.py from manually):
    python agents/inventory/run_hourly.py

Stop with Ctrl+C (or SIGTERM) - the scheduler shuts down cleanly rather
than killing a run mid-flight on the next loop check.
"""

import logging
import signal
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

AGENT_PATH = Path(__file__).resolve().parent / "agent.py"
LOG_DIR = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

# Hard cap well under the hourly cadence, so a hung run can never
# overlap with - or block - the next scheduled trigger.
RUN_TIMEOUT_SECONDS = 50 * 60

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [run_hourly] %(levelname)s %(message)s",
)
log = logging.getLogger("run_hourly")


def run_once() -> None:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log_file = LOG_DIR / f"run_{timestamp}.log"

    log.info("Starting scheduled run -> %s", log_file.name)
    try:
        result = subprocess.run(
            [sys.executable, str(AGENT_PATH)],
            capture_output=True,
            text=True,
            timeout=RUN_TIMEOUT_SECONDS,
        )
        log_file.write_text(
            result.stdout + "\n--- STDERR ---\n" + result.stderr
        )

        if result.returncode == 0:
            log.info("Run completed successfully (exit 0)")
        else:
            log.error(
                "Run exited with code %s - see %s", result.returncode, log_file
            )

    except subprocess.TimeoutExpired:
        log.error(
            "Run exceeded %ss and was killed - will retry at the next scheduled hour",
            RUN_TIMEOUT_SECONDS,
        )
    except Exception:
        log.exception("Wrapper failed to launch agent.py - will retry next hour")


def main() -> None:
    if not AGENT_PATH.exists():
        log.error("agent.py not found at %s - check the path", AGENT_PATH)
        sys.exit(1)

    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        run_once,
        trigger=CronTrigger(minute=0),  # top of every hour, UTC
        id="inventory_agent_hourly",
        max_instances=1,  # never start a new run while one is still in flight
        coalesce=True,    # if the process was asleep and missed runs piled up, just run once
        misfire_grace_time=300,
    )

    def _shutdown(signum, frame):
        log.info("Shutdown signal received, stopping scheduler...")
        scheduler.shutdown(wait=False)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    log.info("Running an initial pass immediately, then hourly at :00 UTC")
    run_once()

    log.info("Scheduler armed - waiting for the next hourly trigger")
    scheduler.start()


if __name__ == "__main__":
    main()
