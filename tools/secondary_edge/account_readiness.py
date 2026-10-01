"""Check account webpage readiness without generating or claiming work."""
from __future__ import annotations

import argparse
from pathlib import Path


def _load_chrome_api():
    # Import only after argument parsing. --help never opens a browser or needs
    # the installed application dependencies.
    from tools.run_codex_web_bridge import (
        CdpConnection,
        COMPOSER_STATE_SCRIPT,
        DeepSeekChromeSession,
    )

    return DeepSeekChromeSession, CdpConnection, COMPOSER_STATE_SCRIPT


def check_deepseek_readiness(profile: Path) -> int:
    """Return 0 for DOM login readiness, 10 for pending login, 20 on error.

    This does not submit a prompt, claim an AI queue item, or establish history
    continuity. Provider details and exceptions are intentionally not printed.
    """
    try:
        session_type, connection_type, state_script = _load_chrome_api()
        page = session_type(profile).page()
        connection = connection_type(page["webSocketDebuggerUrl"])
        try:
            state = connection.evaluate(state_script) or {}
            return 0 if state.get("ready") and not state.get("captcha") else 10
        finally:
            connection.close()
    except Exception:
        return 20


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Check DeepSeek webpage login readiness without generating a response."
    )
    parser.add_argument("--profile", required=True, type=Path)
    args = parser.parse_args(argv)
    return check_deepseek_readiness(args.profile)


if __name__ == "__main__":
    raise SystemExit(main())
