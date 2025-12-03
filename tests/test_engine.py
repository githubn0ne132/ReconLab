import pytest
from sqlmodel import Session, select, create_engine, SQLModel
from app.models import ReconciliationTask, Project
from app.engine import run_api_worker
from sqlalchemy import text, JSON, Column
from unittest.mock import AsyncMock, patch
import os

# Use in-memory DB for testing
@pytest.fixture(name="session")
def session_fixture():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session

def test_json_none_storage(session):
    # Test that None is stored as NULL with the new model definition
    proj = Project(name="Test", mode="API")
    session.add(proj)
    session.commit()

    task = ReconciliationTask(
        project_id=proj.id,
        target_data={"id": 1},
        candidate_data=None,
        status="Pending"
    )
    session.add(task)
    session.commit()

    # Verify raw storage
    result = session.connection().execute(text("SELECT typeof(candidate_data) FROM reconciliationtask WHERE id = :id"), {"id": task.id}).scalar()
    # Should be 'null' (SQL NULL type) or 'text' if "null" string.
    # In SQLite: typeof(NULL) -> 'null'.
    assert result == 'null', f"Expected SQL NULL storage, got {result}"

def test_query_finds_none(session):
    # Test the query logic used in run_api_worker
    from sqlalchemy import cast, String
    from sqlmodel import or_

    proj = Project(name="Test", mode="API")
    session.add(proj)
    session.commit()

    task = ReconciliationTask(
        project_id=proj.id,
        target_data={"id": 1},
        candidate_data=None,
        status="Pending"
    )
    session.add(task)
    session.commit()

    # Query
    statement = select(ReconciliationTask).where(
        ReconciliationTask.project_id == proj.id,
        or_(
            ReconciliationTask.candidate_data == None,
            cast(ReconciliationTask.candidate_data, String) == 'null'
        )
    )
    results = session.exec(statement).all()
    assert len(results) == 1
    assert results[0].id == task.id

def test_query_finds_legacy_null_string(session):
    # Simulate legacy data where None was stored as "null" string
    # We need to bypass the model to insert "null" string if the model enforces NULL now?
    # Or we can insert using raw SQL.

    proj = Project(name="Test Legacy", mode="API")
    session.add(proj)
    session.commit()

    # Insert raw
    session.connection().execute(
        text("INSERT INTO reconciliationtask (project_id, target_data, candidate_data, status) VALUES (:pid, '{}', 'null', 'Pending')"),
        {"pid": proj.id}
    )
    session.commit()

    # Verify it is "null" string
    typeof = session.connection().execute(text("SELECT typeof(candidate_data) FROM reconciliationtask WHERE project_id = :pid"), {"pid": proj.id}).scalar()
    assert typeof == 'text'

    # Query using the engine logic
    from sqlalchemy import cast, String
    from sqlmodel import or_

    statement = select(ReconciliationTask).where(
        ReconciliationTask.project_id == proj.id,
        or_(
            ReconciliationTask.candidate_data == None,
            cast(ReconciliationTask.candidate_data, String) == 'null'
        )
    )
    results = session.exec(statement).all()
    assert len(results) == 1
    # Note: When fetched back, SQLAlchemy JSON type might convert "null" string to None or keep it?
    # If none_as_null=True, loading "null" string might result in None?
    # Let's check.
    assert results[0].candidate_data is None
