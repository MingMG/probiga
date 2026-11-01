"""Generate real research samples without database writes or notifications."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default="")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--facts", help="显式重放已验证事实JSON；日期必须与 --date 一致")
    parser.add_argument("--resume", action="store_true", help="从已完成的盘后研究检查点继续早报")
    args = parser.parse_args()
    from biz.evening_review.generate import resolve_review_trade_date
    from biz.review.quant_digest import load_quant_digest_inputs, generate_quant_digest_from_frames
    from biz.review.research_flow import assert_digest, attach_review, generate_briefing
    from server.common.batch_db import create_batch_engine

    engine = create_batch_engine(connect_args={"connect_timeout": 8, "read_timeout": 45, "write_timeout": 30})
    target = resolve_review_trade_date(engine, args.date)
    output = Path(args.output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    facts_path = output / "verified_facts.json"
    print(f"RUNNING stage=facts completed=0 total=3 target={target}", flush=True)
    if args.resume:
        from biz.evening_review.generate import published_report
        digest = json.loads((output / "digest.json").read_text(encoding="utf-8"))
        published_report(digest, target)
    elif args.facts:
        digest = json.loads(Path(args.facts).read_text(encoding="utf-8"))
    else:
        inputs = load_quant_digest_inputs(engine, target)
        digest = generate_quant_digest_from_frames(target, **inputs)
        facts_path.write_text(json.dumps(digest, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    assert_digest(digest, target)
    if not args.resume:
        facts_path.write_text(json.dumps(digest, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"RUNNING stage=review completed=1 total=3 checkpoint={facts_path}", flush=True)
    def completion_for(kind):
        attempt = 0
        def complete(messages):
            nonlocal attempt
            attempt += 1
            from biz.review.market_decision import _complete
            print(f"RUNNING stage={kind}_model attempt={attempt}", flush=True)
            (output / f"{kind}_input.json").write_text(
                json.dumps(messages, ensure_ascii=False, indent=2), encoding="utf-8")
            (output / f"{kind}_attempt_{attempt}_input.json").write_text(
                json.dumps(messages, ensure_ascii=False, indent=2), encoding="utf-8")
            raw = _complete(messages)
            (output / f"{kind}_response.json").write_text(raw, encoding="utf-8")
            (output / f"{kind}_attempt_{attempt}_response.json").write_text(raw, encoding="utf-8")
            return raw
        return complete
    if not args.resume:
        attach_review(engine, digest, completion=completion_for("review"))
    (output / "digest.json").write_text(json.dumps(digest, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    (output / "review.md").write_text(digest["compact_review"], encoding="utf-8")
    print("RUNNING stage=briefing completed=2 total=3", flush=True)
    briefing = generate_briefing(engine, target, persist=False, digest=digest,
                                 completion=completion_for("briefing"))
    (output / "briefing.json").write_text(json.dumps(briefing, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    (output / "briefing.md").write_text(briefing["content"], encoding="utf-8")
    print(f"COMPLETE completed=3 total=3 review={target} next={briefing['next_trade_date']}", flush=True)
    engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
