"""
Executes agent scripts as isolated subprocesses and guarantees that the
same agent never runs twice concurrently, whether the second attempt
comes from the hourly schedule or a manual rerun request.
"""

import asyncio
import logging
import subprocess
import sys
import time
from pathlib import Path

from agents_registry import AGENTS

LOG_DIR = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logger = logging.getLogger("agent_runner")
logger.setLevel(logging.INFO)
if not logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(_handler)

# How long a single agent run is allowed to take before it's killed.
# Adjust to whatever is realistic for these agents.
AGENT_TIMEOUT_SECONDS = 30 * 60

# One asyncio.Lock per agent. This is what actually prevents a scheduled
# run and a manually-triggered rerun from executing the same script at
# the same time -- APScheduler's max_instances only protects a single
# job id, not two different jobs calling the same function.
_locks: dict[str, asyncio.Lock] = {name: asyncio.Lock() for name in AGENTS}


def is_running(agent_name: str) -> bool:
    """Non-blocking check, used by the API to reject a rerun request
    immediately (409) instead of silently queuing it."""
    return _locks[agent_name].locked()


def _execute(agent_name: str, script_path: str) -> None:
    """Blocking subprocess call. Always run this inside a worker thread
    (see run_agent below) so it never blocks the event loop."""
    log_file = LOG_DIR / f"{agent_name}.log"
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    started = time.monotonic()
    logger.info("Starting agent '%s' (%s)", agent_name, script_path)

    try:
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            timeout=AGENT_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        logger.error("Agent '%s' timed out after %ss", agent_name, AGENT_TIMEOUT_SECONDS)
        with open(log_file, "a") as f:
            f.write(f"\n--- {timestamp}: TIMEOUT after {AGENT_TIMEOUT_SECONDS}s ---\n")
        return

    duration = time.monotonic() - started
    with open(log_file, "a") as f:
        f.write(f"\n--- {timestamp} ({duration:.1f}s, exit={result.returncode}) ---\n")
        if result.stdout:
            f.write(result.stdout)
        if result.stderr:
            f.write("\n[stderr]\n" + result.stderr)

    if result.returncode != 0:
        logger.error(
            "Agent '%s' exited with code %s -- see %s",
            agent_name, result.returncode, log_file,
        )
    else:
        logger.info("Agent '%s' finished successfully in %.1fs", agent_name, duration)


async def run_agent(agent_name: str) -> None:
    """Run one agent's script, serialized per-agent via its lock.

    Used both by the hourly APScheduler jobs and by the manual rerun
    endpoint, so there is exactly one code path for "actually run this
    agent" no matter what triggered it.
    """
    if agent_name not in AGENTS:
        raise KeyError(f"Unknown agent: {agent_name}")

    async with _locks[agent_name]:
        await asyncio.to_thread(_execute, agent_name, AGENTS[agent_name])
