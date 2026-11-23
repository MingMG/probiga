import pytest
from sqlalchemy import create_engine,text
from server.common.native_job_schema import validate_native_job_schema,privileged_migrate_native_job_schema


def test_physical_queue_contract_requires_nullable_owner_clock_and_exact_ready_index():
    engine=create_engine('sqlite://')
    with engine.begin() as connection:
        connection.execute(text('CREATE TABLE st_job_v2(job_id VARCHAR(32),job_type VARCHAR(32),status VARCHAR(32),requested_at DATETIME,next_run_at DATETIME,scheduler_run_uid VARCHAR(64))'))
    with pytest.raises(RuntimeError,match='idx_job_native_ready'):validate_native_job_schema(engine)
    with engine.begin() as connection:
        connection.execute(text('CREATE INDEX idx_job_native_ready ON st_job_v2(job_type,status,next_run_at,requested_at,job_id)'))
    assert validate_native_job_schema(engine)['valid']
    with engine.begin() as connection:
        connection.execute(text('DROP INDEX idx_job_native_ready'))
        connection.execute(text('CREATE INDEX idx_job_native_ready ON st_job_v2(status,job_type,next_run_at,requested_at,job_id)'))
    with pytest.raises(RuntimeError,match='idx_job_native_ready'):validate_native_job_schema(engine)
    engine.dispose()


def test_migration_does_not_silently_run_a_different_database_boundary():
    engine=create_engine('sqlite://')
    with pytest.raises(RuntimeError,match='requires MySQL'):privileged_migrate_native_job_schema(engine)
    engine.dispose()
