# A-share briefing tools

This surface exposes ten bounded, read-only tools to the A-share briefing
assistant.  It is mounted on the existing FastAPI application; it does not give
the model database credentials or permit arbitrary SQL.

## Enable

Set the following values in the server environment:

```dotenv
PROBIGA_BRIEFING_TOOLS_ENABLED=true
PROBIGA_BRIEFING_TOOLS_TOKEN=<long-random-token>
PROBIGA_BRIEFING_TOOLS_TIMEOUT_SECONDS=10
PROBIGA_BRIEFING_TOOLS_MAX_ROWS=200
BIG_QMT_BRIDGE_ENABLED=true
```

The token is independent from `PROBIGA_ADMIN_TOKEN`.  Every tool call requires
`Authorization: Bearer <PROBIGA_BRIEFING_TOOLS_TOKEN>`.

`BIG_QMT_BRIDGE_ENABLED=true` is the existing Windows/QMT collection path.
Event-time precision is bounded by `BIG_QMT_FULL_REFRESH_SECONDS` (default 30
seconds).  Queue snapshots are emitted at most once per minute while a stock
remains sealed.

The filtered OpenAPI document for an AI function client is:

```text
GET /api/briefing-tools/openapi.json
```

It contains only the ten `get_*` operations and never includes the application's
mutation, deployment, scheduler or trading routes.

## Tools

| Operation ID | HTTP path |
|---|---|
| `get_index_snapshot` | `/api/briefing-tools/index-snapshot` |
| `get_sector_rank` | `/api/briefing-tools/sector-rank` |
| `get_stock_rank` | `/api/briefing-tools/stock-rank` |
| `get_watchlist` | `/api/briefing-tools/watchlist` |
| `get_announcements` | `/api/briefing-tools/announcements` |
| `get_market_breadth` | `/api/briefing-tools/market-breadth` |
| `get_intraday_snapshot` | `/api/briefing-tools/intraday-snapshot` |
| `get_limit_up_events` | `/api/briefing-tools/limit-up-events` |
| `get_sector_linkage` | `/api/briefing-tools/sector-linkage` |
| `get_market_breadth_intraday` | `/api/briefing-tools/market-breadth-intraday` |

All responses use:

```json
{
  "schema_version": "probiga.a-share-briefing-tools.v1",
  "tool": "get_index_snapshot",
  "request_id": "client-or-generated-id",
  "as_of": "2026-10-23",
  "data_status": "AVAILABLE",
  "missing_fields": [],
  "data": [],
  "meta": {}
}
```

`data_status` is one of `AVAILABLE`, `PARTIAL` or `NO_DATA`.  A `PARTIAL`
response must not be described as complete.  `missing_fields` identifies the
fields the assistant must render as `未获取`.

## Data contracts

- Index snapshots come from `sm_index_kline` with index names from
  `si_all_index_code`.
- Sector rankings combine `sm_concept_east_kline`,
  `sm_concept_capital_flow_east`, `si_concept_code_east`,
  `si_concept_constituent_east` and `sm_stock_kline`.
- Stock rankings combine `sm_stock_kline`,
  `sm_stock_capital_flow_daily` and verified concept membership.
- `volume_ratio_20d` is explicitly calculated as current volume divided by the
  previous 20 trading-day average.  It is not an exchange-provided instantaneous
  volume-ratio field.
- Board-rule limit status compares close with the upper limit derived from
  pre-close, board, and ST status.  Responses identify this basis and must not
  claim provider-supplied exact limit-price evidence.
- Announcements come from validated `si_notice_eastmoney` title records.
- Intraday stock data uses the configured minute table.  Concept intraday data
  uses `sm_concept_east_minute`.
- Sector linkage includes explicit `time_delta_vs_sector_seconds`; the model
  may describe causality only when the movement times support it.

## Limit-up event capture

`sm_limit_up_event` is append-only and is populated by the active BigQMT bridge
from validated `ContextInfo.get_full_tick` snapshots:

- `TOUCH`
- `SEAL`
- `BREAK`
- `RESEAL`
- `QUEUE_SNAPSHOT`

The event record preserves event time, provider snapshot time, price, pre-close,
derived upper limit, first bid/ask levels, raw first-bid volume, prior raw
first-bid volume and an evidence hash key.  It deliberately does not convert
provider volume into "seal amount" because QMT transports do not all expose the
same quantity unit.

The table and API are coordinated release artifacts:

- Linux/server: FastAPI routes, read schema, API token and database grant.
- Windows/QMT: BigQMT event writer and full-tick snapshot runtime.

Both endpoints must receive the same merged revision.  A Linux-only release must
not advertise event history as complete before the Windows radar writer is
active.

If the BigQMT bridge is disabled or has not yet observed a lifecycle, the event and
intraday-break tools return `NO_DATA` or `PARTIAL`; they never backfill exact
touch/break times from daily bars.

## Database grant

Use a database principal that can only execute `SELECT` against the required
tables or, preferably, matching read-only views.  Do not grant `INSERT`,
`UPDATE`, `DELETE`, `ALTER`, `CREATE`, `DROP` or `EXECUTE` to the tool principal.
The event writer uses the existing QMT collector principal, not the read-only
tool principal.

## Verification

The deterministic tests cover authentication, exact operation IDs, filtered
OpenAPI, invalid scope rejection, the 200-row ceiling, board and ST limit rules,
and touch/seal/break/reseal/queue ordering.  A production canary still needs to
prove the following after the coordinated release:

1. Each endpoint authenticates with the dedicated token.
2. Each response is bounded to at most 200 rows and reports `PARTIAL` when
   required fields are absent.
3. The Windows BigQMT bridge writes a real touch/break lifecycle event for a
   controlled stock and the Linux reader returns it with matching source
   timestamps.
4. Disabling the tool flag closes access without restarting unrelated QMT or
   scheduler components.
