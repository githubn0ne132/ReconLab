import subprocess
import time
import os
import signal
import pytest
from playwright.sync_api import Page, expect
from sqlmodel import Session, select
import httpx
from app.db import engine, create_db_and_tables
from app.models import Project

# Define a fixture for the running app
@pytest.fixture(scope="module")
def app_server():
    # Setup: Clear and populate DB
    # Ensure tables exist
    create_db_and_tables()

    project_id = None
    with Session(engine) as session:
        # Cleanup existing test project if any
        existing = session.exec(select(Project).where(Project.name == "Test Project UI")).first()
        if existing:
            session.delete(existing)
            session.commit()

        # Create a new test project
        project = Project(
            name="Test Project UI",
            mode="CSV",
            status="Mapping",
            target_table_name="test_target",
            source_table_name="test_source"
        )
        session.add(project)
        session.commit()
        session.refresh(project)
        project_id = project.id

    # Start the app
    # We use uv run to ensure environment
    # Sanitize environment to prevent NiceGUI from thinking it's in a test runner
    env = os.environ.copy()
    if "PYTEST_CURRENT_TEST" in env:
        del env["PYTEST_CURRENT_TEST"]

    proc = subprocess.Popen(
        ["uv", "run", "main.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=os.getcwd(), # Root
        env=env, # Pass sanitized env
        preexec_fn=os.setsid if os.name != 'nt' else None # Create process group to kill easier
    )

    # Wait for app to start
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
        # Print stdout/stderr
        try:
            stdout, stderr = proc.communicate(timeout=1)
        except subprocess.TimeoutExpired:
            proc.kill()
            stdout, stderr = proc.communicate()

        print("App failed to start:")
        print(stdout.decode())
        print(stderr.decode())
        if proc.poll() is None:
            proc.kill()
        pytest.fail("App failed to start")

    yield {"proc": proc, "project_id": project_id, "url": url}

    # Teardown
    if os.name != 'nt':
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except ProcessLookupError:
            pass
    else:
        proc.terminate()
    proc.wait()

    # Cleanup DB
    with Session(engine) as session:
        p = session.get(Project, project_id)
        if p:
            session.delete(p)
            session.commit()

def test_resume_and_delete(page: Page, app_server):
    project_id = app_server["project_id"]
    base_url = app_server["url"]

    # Go to home
    page.goto(base_url)

    # Verify project is listed
    expect(page.get_by_text("Test Project UI")).to_be_visible()

    # TEST RESUME
    # Find the row
    row = page.get_by_role("row", name="Test Project UI")
    # Find the resume button (play_arrow icon)
    resume_btn = row.locator("button:has(.q-icon:has-text('play_arrow'))")
    expect(resume_btn).to_be_visible()

    # Click resume
    resume_btn.click()

    # Verify navigation to mapping page
    expect(page).to_have_url(f"{base_url}/mapping/{project_id}")

    # Go back to home
    page.goto(base_url)
    expect(page.get_by_text("Test Project UI")).to_be_visible()

    # TEST DELETE
    row = page.get_by_role("row", name="Test Project UI")
    delete_btn = row.locator("button:has(.q-icon:has-text('delete'))")
    expect(delete_btn).to_be_visible()

    delete_btn.click()

    # Verify row disappears
    # It might take a moment
    expect(page.get_by_text("Test Project UI")).not_to_be_visible(timeout=5000)
