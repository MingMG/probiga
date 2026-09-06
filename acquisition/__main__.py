"""Explicit-config commands for the new acquisition layer; no implicit .env."""
import argparse
import json
import time

from .config import Config, DirectEtfWriterDisabled, require_supported_writer_datasets
from .datasets import get_spec
from .runner import Runner


def parser():
    root = argparse.ArgumentParser(description="Direct data acquisition (writes disabled unless explicitly configured)")
    root.add_argument("--config", required=True, help="explicit protected configuration JSON path")
    commands = root.add_subparsers(dest="command", required=True)
    daily = commands.add_parser("daily", help="process due configured historical/event datasets")
    daily.add_argument("--datasets", nargs="+", help="enabled dataset names, space or comma separated")
    daily.add_argument("--date", default="latest")
    daily.add_argument("--budget-seconds", type=float, default=1200)
    backfill = commands.add_parser("backfill", help="bounded explicit historical date range")
    backfill.add_argument("--start", required=True)
    backfill.add_argument("--end", required=True)
    backfill.add_argument("--datasets", nargs="+")
    backfill.add_argument("--budget-seconds", type=float, default=1200)
    status = commands.add_parser("status", help="read-only dataset progress")
    status.add_argument("--datasets", nargs="+")
    status.add_argument("--date", default="latest")
    status.add_argument("--json", action="store_true", help="output is always JSON")
    live = commands.add_parser("live", help="consume native snapshots; never download history")
    mode = live.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true")
    mode.add_argument("--duration-seconds", type=float, help="bounded loop, at most 300 seconds")
    reference = commands.add_parser("reference", help="request an explicit native reference batch")
    reference.add_argument("--asset-class", choices=("stock", "index", "etf"), required=True)
    reference.add_argument("--codes", required=True, help="comma-separated qualified symbols or explicit sector names")
    reference.add_argument("--period", choices=("instrument", "sector", "calendar"), default="instrument")
    reference.add_argument("--target", help="YYYY-MM-DD")
    return root


def _datasets(args, config):
    enabled = list(config.data.get("datasets", []))
    requested = getattr(args, "datasets", None)
    selected = [part.strip() for item in requested for part in item.split(",") if part.strip()] if requested else enabled
    selected = list(dict.fromkeys(selected))
    require_supported_writer_datasets(selected)
    if any(name not in enabled for name in selected):
        raise ValueError("requested dataset is not enabled in explicit configuration")
    if args.command in ("daily", "backfill"):
        unsupported = [name for name in selected if get_spec(name).period == "tick" or name == "reference"]
        if requested and unsupported:
            raise ValueError("live and reference require their dedicated commands")
        selected = [name for name in selected if name not in unsupported]
    if not selected:
        raise ValueError("no enabled datasets for this command")
    return selected


def _print(value):
    print(json.dumps(value, ensure_ascii=False, default=str, allow_nan=False), flush=True)


def main(argv=None):
    args = parser().parse_args(argv)
    runner = None
    try:
        config = Config.load(args.config)
        if args.command != "status":
            config.require_writes()
        budget = getattr(args, "budget_seconds", 1200)
        if not 0 < budget <= 1200:
            raise ValueError("run budget must be positive and at most 1200 seconds")
        duration = getattr(args, "duration_seconds", None)
        if duration is not None and not 0 < duration <= 300:
            raise ValueError("live duration must be positive and at most 300 seconds")
        runner = Runner(config)
        if args.command == "daily":
            result = runner.run(_datasets(args, config), requested=args.date,
                                budget_seconds=budget, due=True)
        elif args.command == "backfill":
            from datetime import date
            if date.fromisoformat(args.start) > date.fromisoformat(args.end):
                raise ValueError("backfill start follows end")
            result = runner.run(_datasets(args, config), start=args.start, end=args.end,
                                budget_seconds=budget)
        elif args.command == "status":
            result = runner.status(_datasets(args, config), requested=args.date)
        elif args.command == "reference":
            codes = [code.strip() for code in args.codes.split(",") if code.strip()]
            if not codes or len(codes) != len(set(codes)):
                raise ValueError("reference codes must be nonempty and unique")
            result = runner.reference(args.asset_class, codes, period=args.period, target=args.target)
        else:
            if duration is None:
                result = runner.live_once()
            else:
                deadline = time.monotonic() + duration
                while True:
                    result = runner.live_once()
                    _print(result)
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return 0
                    time.sleep(min(15.0, remaining))
                    if time.monotonic() >= deadline:
                        return 0
        _print(result)
        return 2 if isinstance(result, dict) and result.get("status") in {"partial", "unavailable", "error"} else 0
    except KeyboardInterrupt:
        _print({"status": "interrupted", "error": "KeyboardInterrupt"})
        return 130
    except DirectEtfWriterDisabled as exc:
        _print({"status": "error", "error": type(exc).__name__, "message": str(exc)})
        return 2
    except Exception as exc:
        # Provider/SQL exceptions may embed account identifiers or connection
        # strings. Detailed safe unit errors remain in the normal status store.
        _print({"status": "error", "error": type(exc).__name__})
        return 2
    finally:
        if runner is not None:
            runner.close()


if __name__ == "__main__":
    raise SystemExit(main())
