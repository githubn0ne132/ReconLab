import subprocess
import time
import os
import signal
import pytest
from playwright.sync_api import Page, expect
from sqlmodel import Session, select
import httpx
from app.db import engine, create_db_and_tables
from app.models import Project, ReconciliationTask

@pytest.fixture(scope="module")
def app_server():
    create_db_and_tables()
    project_id = None
    with Session(engine) as session:
        # Cleanup
        existing = session.exec(select(Project).where(Project.name == "Test API Project")).first()
        if existing:
            session.delete(existing)
            session.commit()

        project = Project(
            name="Test API Project",
            mode="API",
            status="Processing",
            target_table_name="test_target",
            mapping_config={"api_token": "fake", "join_key": {"target": "siret"}}
        )
        session.add(project)
        session.commit()
        session.refresh(project)
        project_id = project.id

        # Add a task
        task = ReconciliationTask(
            project_id=project.id,
            target_data={"siret": "123"},
            candidate_data=None,
            status="Pending"
        )
        session.add(task)
        session.commit()

    env = os.environ.copy()
    if "PYTEST_CURRENT_TEST" in env:
        del env["PYTEST_CURRENT_TEST"]
    env["DUCKDB_FILE"] = "test_ui_val.duckdb"

    proc = subprocess.Popen(
        ["uv", "run", "main.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=os.getcwd(),
        env=env,
        preexec_fn=os.setsid if os.name != 'nt' else None
    )

    start_time = time.time()
    url = "http://localhost:8080"
    started = False
    while time.time() - start_time < 20:
        try:
            r = httpx.get(url)
            if r.status_code == 200:
                started = True
                break
        except:
            time.sleep(1)

    if not started:
        proc.kill()
        pytest.fail("App failed to start")

    yield {"proc": proc, "project_id": project_id, "url": url}

    if os.name != 'nt':
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except:
            pass
    else:
        proc.terminate()
    proc.wait()

    with Session(engine) as session:
        p = session.get(Project, project_id)
        if p:
            session.delete(p)
            session.commit()

def test_validation_page_loads(page: Page, app_server):
    project_id = app_server["project_id"]
    base_url = app_server["url"]

    page.goto(f"{base_url}/validation/{project_id}")

    expect(page.get_by_text("Validation: Test API Project")).to_be_visible()

    # Expect Task ID to appear (meaning fetching attempt finished/failed and page rendered)
    expect(page.get_by_text("Task ID:")).to_be_visible(timeout=10000)
