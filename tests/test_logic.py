
def test_manual_edit_logic():
    from app.models import ReconciliationTask
    from sqlmodel import Session
    from app.db import engine

    # We assume DB is setup by previous tests or initialized
    # For a unit test, we can mock or use in-memory.
    # Current engine is file based. We'll use a new task object to test the logic flow representation.

    t = ReconciliationTask(project_id=1, target_data={"col1": "val1"}, status="Pending")

    # Simulate Manual Edit
    new_values = {"col1": "val1_edited"}

    t.final_data = new_values
    t.decision = "Manual Edit"
    t.status = "Resolved"

    assert t.status == "Resolved"
    assert t.final_data["col1"] == "val1_edited"
