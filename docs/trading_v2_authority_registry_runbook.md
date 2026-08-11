# V2 authority registry TEST/CI runbook

This command manages only the four append-only migration-014 registry records:
trust keys, signed receipts, key revocations, and receipt revocations. It does
not write accounts, orders, fills, positions, cash, recommendations, or any
production/actionable output.

## Safety model

Running without `--apply` is the normal path. It reads and validates one local
JSON file, derives the binding hashes, prints a redacted preview, and does not
resolve a database environment variable or open a connection.

An apply is allowed only when all of these conditions pass in one caller-owned
transaction:

- `--environment` is exactly `TEST` or `CI`;
- the URL and expected server UUID come from the dedicated variables below;
- the URL selects Oracle MySQL 5.7.38 on the independently supplied UUID;
- the schema name is explicitly `*_v2_evidence_test*` or
  `*_v2_evidence_ci*`, as appropriate, and the URL has no production identity;
- the migration ledger is exactly 011 through 015 and matches independent,
  code-frozen checksums (not merely checksums recalculated from live code);
- the complete V2 evidence schema/trigger gate passes and the maintenance
  fence is locked `INACTIVE`;
- the independent authority stored-row auditor passes before and after the
  append, including database `SHA2`, Ed25519 signatures, parents, and shared
  row locks.

Any refusal or exception rolls back the whole transaction. `INSERTED` and an
exact `IDEMPOTENT` replay are the only successful writer outcomes. Conflicting
primary identities, reused key material, claim/envelope/nonce reuse, or a
different revocation binding fail closed.

## Operation documents

The outer object must contain exactly `operation` and `payload`. Timestamps
must include a UTC offset. SHA-256 values are lowercase hexadecimal. Identity
text is case-sensitive ASCII. JSON duplicate keys, unknown fields, padded or
non-canonical key encodings, and non-finite numbers are rejected.

Trust-key registration uses a canonical unpadded base64url Ed25519 public key:

```json
{
  "operation": "TRUST_KEY",
  "payload": {
    "source_provider": "example-provider",
    "key_id": "provider-key",
    "key_version": "2026-08",
    "public_key_base64url": "<43-character-base64url-public-key>",
    "valid_from": "2026-08-04T00:00:00.000000+00:00",
    "valid_to": "2026-09-04T00:00:00.000000+00:00"
  }
}
```

Receipt registration supplies the exact authority claim and the canonical
`SignedAuthorityReceipt.envelope_json` string. The envelope binds the claim,
provider, receipt, key/version, nonce, issue/expiry times, and Ed25519
signature.

```json
{
  "operation": "RECEIPT",
  "payload": {
    "claim": {
      "evidence_type": "MARKET_CALENDAR",
      "evidence_id": "<64-lowercase-hex>",
      "source_provider": "example-provider",
      "source_payload_hash": "<64-lowercase-hex>",
      "receipt_type": "CALENDAR_OTHER",
      "receipt_id": "calendar-receipt-1",
      "receipt_hash": "<64-lowercase-hex>",
      "available_at": "2026-08-04T01:00:00.000000+00:00",
      "trade_date": "2026-08-04",
      "event_at": null,
      "received_at": null
    },
    "envelope_json": "<exact canonical signed envelope JSON>"
  }
}
```

The receipt-type matrix is closed:

| Evidence type | Allowed receipt type |
|---|---|
| `MARKET_CALENDAR` | `CALENDAR_OTHER` |
| `INSTRUMENT_RULE` | `INSTRUMENT_RULE` |
| `QUOTE_RECEIPT` | `QMT_MINUTE`, `QMT_REALTIME`, `PUBLIC_CONSENSUS`, `OTHER` |

Key revocation:

```json
{
  "operation": "KEY_REVOCATION",
  "payload": {
    "source_provider": "example-provider",
    "key_id": "provider-key",
    "key_version": "2026-08",
    "revoked_at": "2026-08-20T00:00:00.000000+00:00",
    "reason_code": "KEY_ROTATED"
  }
}
```

Receipt revocation must carry the exact immutable receipt/envelope binding:

```json
{
  "operation": "RECEIPT_REVOCATION",
  "payload": {
    "receipt_id": "calendar-receipt-1",
    "receipt_hash": "<64-lowercase-hex>",
    "envelope_hash": "<64-lowercase-hex>",
    "revoked_at": "2026-08-20T00:00:00.000000+00:00",
    "reason_code": "SOURCE_RETRACTED"
  }
}
```

`registered_at` and every registry `created_at` are owned by the MySQL
triggers (`UTC_TIMESTAMP(6)`) and are therefore absent from operation input.
The writer reads their exact stored values back in the same transaction.

## Preview and apply

Always preview the exact file first:

```powershell
.venv\Scripts\python.exe tools\manage_v2_authority_registry.py .\operation.json
```

For TEST only:

```powershell
$env:V2_EVIDENCE_TEST_AUTHORITY_MYSQL_URL = '<dedicated-test-mysql-url>'
$env:V2_EVIDENCE_TEST_AUTHORITY_MYSQL_SERVER_UUID = '<canonical-server-uuid>'
.venv\Scripts\python.exe tools\manage_v2_authority_registry.py .\operation.json --apply --environment TEST
```

For CI, use `V2_EVIDENCE_CI_AUTHORITY_MYSQL_URL` and
`V2_EVIDENCE_CI_AUTHORITY_MYSQL_SERVER_UUID`, then pass `--environment CI`.
There is intentionally no URL flag, generic `DATABASE_URL`/`MYSQL_URL`
fallback, production environment, force switch, schema bootstrap, or migration
execution path.

After an apply, retain the JSON result with the change ticket. It records the
writer status, target database/server UUID, exact ledger versions, and pre/post
authority-audit row counts, but never prints the database URL, password,
public-key bytes, signed envelope, or signature.
