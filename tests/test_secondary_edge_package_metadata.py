"""Pure receipt bindings, tiny fake physical files, no database/server calls."""
import hashlib
import json

import pytest

from tools.secondary_edge import cold_database as c
from tools.secondary_edge import package_metadata as p
from test_secondary_edge_cold_database import source_fixture, stopped


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def metadata_fixture(tmp_path, host="SOURCE"):
    layout, pause, roots = source_fixture(tmp_path)
    layout["source"]["hostname"] = pause["source_host"] = host
    pause["source_qmt_running"] = False
    package = tmp_path / "package"
    metadata = c.snapshot_source(layout, pause, package / "database", runtime_probe=stopped)
    write_json(package / "audit/source-layout.json", layout)
    write_json(package / "audit/source-pause.json", pause)
    files = [{"path": str(file.relative_to(package)).replace("\\", "/"),
              "bytes": file.stat().st_size, "sha256": hashlib.sha256(file.read_bytes()).hexdigest()}
             for file in package.rglob("*") if file.is_file()]
    manifest = {"database": metadata, "source_host": host, "files": files}
    write_json(package / "manifest.json", manifest)
    return package, manifest, layout, pause


def test_structural_binding_does_not_hash_database_twice(tmp_path, monkeypatch):
    package, manifest, _, _ = metadata_fixture(tmp_path)
    monkeypatch.setattr(c, "_digest", lambda *a, **k: pytest.fail("Unexpected full database hash"))
    result = p.validate_package_metadata(package)
    assert result["snapshot_id"] == manifest["database"]["snapshot_id"]
    assert result["status"] == "metadata-verified"


@pytest.mark.parametrize("kind", ["database-object", "pause", "layout", "state", "row-size", "row-sha", "missing", "extra"])
def test_independent_receipts_and_release_rows_cannot_diverge(tmp_path, kind):
    package, manifest, layout, pause = metadata_fixture(tmp_path)
    if kind == "database-object":
        manifest["database"]["source"]["hostname"] = "OTHER"
    elif kind == "pause":
        pause["completed_at_utc"] = "different-time"
        write_json(package / "audit/source-pause.json", pause)
    elif kind == "layout":
        layout["roots"]["data"] = "E:/different-data"
        write_json(package / "audit/source-layout.json", layout)
    elif kind == "state":
        write_json(package / "database/snapshot-state.json", {"status": "COPYING"})
    elif kind in {"row-size", "row-sha"}:
        row = next(row for row in manifest["files"] if row["path"] == "database/data/mysql.ibd")
        row["bytes" if kind == "row-size" else "sha256"] = 999 if kind == "row-size" else "0" * 64
    elif kind == "missing":
        manifest["files"] = [row for row in manifest["files"] if row["path"] != "database/data/mysql.ibd"]
    else:
        manifest["files"].append({"path": "database/data/unexpected", "bytes": 0, "sha256": "0" * 64})
    write_json(package / "manifest.json", manifest)
    with pytest.raises(c.ColdError):
        p.validate_package_metadata(package)


def test_snapshot_id_is_not_a_claimed_string(tmp_path):
    package, manifest, _, _ = metadata_fixture(tmp_path)
    metadata = manifest["database"]
    metadata["source"]["version"] = "8.4.12"
    write_json(package / "database/metadata.json", metadata)
    write_json(package / "manifest.json", manifest)
    with pytest.raises(c.ColdError):
        p.validate_package_metadata(package)
