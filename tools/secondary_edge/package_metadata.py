"""Pure cold-artifact identity binding; never connects to MySQL or a server.

File bytes are authenticated by package_common's complete SHA256 validator.
This module checks the independent snapshot receipt and its release bindings
without a second hash pass over the 150+ GiB database.
"""
import argparse
import json
from pathlib import Path

from .cold_database import ColdError, LAYOUT_FORMAT, _auto_uuid, load_snapshot


def _read(path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def validate_package_metadata(package):
    package = Path(package)
    manifest = _read(package / "manifest.json")
    metadata, database = load_snapshot(package, verify_files=False)
    layout = _read(package / "audit/source-layout.json")
    pause = _read(package / "audit/source-pause.json")
    state = _read(database / "snapshot-state.json")
    if (layout.get("format") != LAYOUT_FORMAT or manifest.get("database") != metadata
            or metadata.get("pause") != pause
            or metadata.get("source") != layout.get("source")
            or metadata.get("source_roots") != layout.get("roots")
            or manifest.get("source_host", "").casefold() != metadata["source"]["hostname"].casefold()
            or state.get("status") != "ready"
            or state.get("snapshot_id") != metadata["snapshot_id"]
            or pause.get("source_qmt_running") is not False
            or _auto_uuid(database / "data/auto.cnf") != metadata["source"]["server_uuid"].lower()):
        raise ColdError("PACKAGE_SNAPSHOT_IDENTITY_BINDING_INVALID")
    files = {}
    for row in manifest.get("files", []):
        path = str(row.get("path", "")).replace("\\", "/").casefold()
        if path in files:
            raise ColdError("PACKAGE_SNAPSHOT_DUPLICATE_RELEASE_ROW")
        files[path] = row
    expected = {"database/metadata.json", "database/snapshot-state.json"}
    for row in metadata["files"]:
        path = f"database/{row['root']}/{row['path']}".replace("\\", "/").casefold()
        released = files.get(path) or {}
        if (row["bytes"] < 0 or released.get("bytes") != row["bytes"]
                or str(released.get("sha256", "")).lower() != row["sha256"]):
            raise ColdError("PACKAGE_SNAPSHOT_RELEASE_ROW_MISMATCH")
        expected.add(path)
    if expected != {path for path in files if path.startswith("database/")}:
        raise ColdError("PACKAGE_SNAPSHOT_RELEASE_FILE_SET_MISMATCH")
    return {"status": "metadata-verified", "snapshot_id": metadata["snapshot_id"],
            "database_files": len(metadata["files"])}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        result = validate_package_metadata(args.package)
    except (ColdError, OSError, ValueError, KeyError, TypeError) as error:
        # No contents, account values or source secrets enter diagnostic output.
        reason = str(error) if isinstance(error, ColdError) else "PACKAGE_METADATA_READ_INVALID"
        parser.exit(2, reason + "\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
