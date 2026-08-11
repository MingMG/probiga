from __future__ import annotations

from sqlalchemy import create_engine, text

from server.common.batch_db import read_frame_direct


def test_read_frame_direct_compiles_named_parameters():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE sample (id INTEGER, name TEXT)"))
        conn.execute(text("INSERT INTO sample (id, name) VALUES (1, 'alpha')"))

    frame = read_frame_direct(
        "SELECT id, name FROM sample WHERE id = :id",
        engine,
        params={"id": 1},
    )

    assert frame.to_dict(orient="records") == [
        {"id": 1, "name": "alpha"},
    ]
