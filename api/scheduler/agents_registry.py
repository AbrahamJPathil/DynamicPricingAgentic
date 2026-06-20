"""
Single source of truth for which agents exist and where their scripts live.

To add a new agent, add one line here -- nothing else in the codebase
needs to change, since main.py and runner.py both read from AGENTS.
"""

from pathlib import Path

# Resolve paths relative to this file so the service works regardless
# of the working directory it's launched from.
BASE_DIR = Path(__file__).resolve().parent.parent.parent

AGENTS = {
    "inventory": str(BASE_DIR / "agents" / "inventory" / "agent.py"),
    "competitor": str(BASE_DIR / "agents" / "competitor" / "agent.py"),
}
