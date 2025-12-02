from sqlmodel import Session, select
from app.models import Project, ReconciliationTask
from app.db import engine
from app.duckdb_client import duckdb_client
from app.sirene import SireneClient
from loguru import logger
from typing import Optional, List, Dict, Any
import json
import asyncio

def initialize_tasks_csv(project_id: int) -> None:
    """
    Initializes reconciliation tasks for a CSV-to-CSV project.
    Performs a Left Join in DuckDB and populates SQLite.
    """
    with Session(engine) as session:
        project = session.get(Project, project_id)
        if not project:
            logger.error(f"Project {project_id} not found.")
            return

        target_table = project.target_table_name
        source_table = project.source_table_name
        mapping = project.mapping_config

        join_key = mapping.get("join_key", {})
        target_key = join_key.get("target")
        source_key = join_key.get("source")

        if not target_key or not source_key:
            logger.error("Invalid join configuration.")
            return

        # Perform Join in DuckDB
        # We select everything from target (t) and source (s)
        # We need to construct the query carefully to avoid column name collisions if we want to separate them later,
        # but for simplicity, we can fetch as dicts.
        # Actually, to store in "target_data" and "candidate_data", it's best to select them separately or struct them.

        # DuckDB Struct approach:
        # SELECT row_to_json(t) as target_json, row_to_json(s) as source_json
        # FROM target t LEFT JOIN source s ON t.key = s.key
        # Note: DuckDB has struct packing.

        query = f"""
        SELECT
            to_json(t) as target_json,
            to_json(s) as source_json
        FROM {target_table} t
        LEFT JOIN {source_table} s
        ON t."{target_key}" = s."{source_key}"
        """

        try:
            results = duckdb_client.query_as_dict(query)

            tasks = []
            for row in results:
                t_json = row.get("target_json")
                s_json = row.get("source_json")

                target_data = json.loads(t_json) if t_json else {}
                candidate_data = json.loads(s_json) if s_json else None

                task = ReconciliationTask(
                    project_id=project.id,
                    target_data=target_data,
                    candidate_data=candidate_data,
                    status="Pending"
                )
                tasks.append(task)

            # Bulk insert might be faster, but SQLModel uses add_all
            session.add_all(tasks)

            project.status = "Processing" # Or "Validation" if immediate
            session.add(project)
            session.commit()

            logger.info(f"Initialized {len(tasks)} tasks for Project {project_id}")

        except Exception as e:
            logger.error(f"Failed to initialize CSV tasks: {e}")


def initialize_tasks_api_pre(project_id: int) -> None:
    """
    Initializes tasks for API mode.
    Loads Target rows into SQLite with candidate_data = None.
    """
    with Session(engine) as session:
        project = session.get(Project, project_id)
        if not project:
            return

        target_table = project.target_table_name

        # Select all from target
        query = f"SELECT to_json(t) as target_json FROM {target_table} t"

        try:
            results = duckdb_client.query_as_dict(query)
            tasks = []
            for row in results:
                t_json = row.get("target_json")
                target_data = json.loads(t_json) if t_json else {}

                task = ReconciliationTask(
                    project_id=project.id,
                    target_data=target_data,
                    candidate_data=None, # To be filled by worker
                    status="Pending"
                )
                tasks.append(task)

            session.add_all(tasks)
            project.status = "Processing"
            session.add(project)
            session.commit()
            logger.info(f"Initialized {len(tasks)} API placeholder tasks.")

        except Exception as e:
            logger.error(f"Failed to initialize API tasks: {e}")

async def run_api_worker(project_id: int, token: Optional[str] = None) -> None:
    """
    Background worker to fetch API data for pending tasks.
    """
    logger.info(f"Starting API Worker for Project {project_id}")
    client = SireneClient(token)

    # We need a new session for the thread/async task
    # Note: SQLModel with async is tricky if not using AsyncSession.
    # For simplicity in this local app, we will use sync session in short bursts or refactor if needed.
    # NiceGUI runs on an event loop, so we can use async functions.

    # We'll batch process or iterate
    # Ideally, we fetch pending tasks from DB

    with Session(engine) as session:
        project = session.get(Project, project_id)
        if not project:
            return

        mapping = project.mapping_config
        target_key_col = mapping.get("join_key", {}).get("target")

        # Get tasks where candidate_data is None (or status is Pending and we haven't tried?)
        # For now, let's assume we process all "Pending" tasks that have no candidate data (and are API mode)
        # But wait, if we process them, they might still be "Pending" validation.
        # We need a flag or just check if candidate_data is empty?
        # Actually the prompt says "Insert all... with candidate_data = null".

        statement = select(ReconciliationTask).where(
            ReconciliationTask.project_id == project_id,
            ReconciliationTask.candidate_data == None
        )
        tasks = session.exec(statement).all()
        logger.info(f"Found {len(tasks)} tasks to process via API.")

    # We shouldn't keep the session open during long API calls.
    # We process in chunks.

    chunk_size = 10
    total = len(tasks)

    for i in range(0, total, chunk_size):
        chunk = tasks[i:i+chunk_size]

        for task in chunk:
            # Re-fetch task to attach to session if needed or just update by ID later
            target_val = task.target_data.get(target_key_col)

            if target_val:
                logger.info(f"Fetching SIRET: {target_val}")
                result = await client.get_by_siret(str(target_val))

                # Update DB
                with Session(engine) as session:
                    t_update = session.get(ReconciliationTask, task.id)
                    if t_update:
                        t_update.candidate_data = result if result else {} # Empty dict if not found, to mark as processed?
                        # Or keep None if we want to retry?
                        # Let's use {} for "Not Found" to distinguish from "Not Attempted" (None)
                        session.add(t_update)
                        session.commit()

                await asyncio.sleep(0.2) # Rate limiting respect (5 calls/sec roughly)

    logger.info("API Worker Finished.")
