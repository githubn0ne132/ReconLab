from sqlmodel import Session, select, or_
from sqlalchemy import cast, String
from app.models import Project, ReconciliationTask
from app.db import engine
from app.duckdb_client import duckdb_client
from app.sirene import SireneClient
from loguru import logger
from typing import Optional, List, Dict, Any
import json
import asyncio
import os

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

            session.add_all(tasks)

            project.status = "Processing"
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

    with Session(engine) as session:
        project = session.get(Project, project_id)
        if not project:
            return

        mapping = project.mapping_config
        target_key_col = mapping.get("join_key", {}).get("target")

        # Select tasks that have no candidate data.
        # Handle both NULL (new behavior) and "null" string (legacy behavior).
        statement = select(ReconciliationTask).where(
            ReconciliationTask.project_id == project_id,
            or_(
                ReconciliationTask.candidate_data == None,
                cast(ReconciliationTask.candidate_data, String) == 'null'
            )
        )
        tasks = session.exec(statement).all()
        logger.info(f"Found {len(tasks)} tasks to process via API.")

    chunk_size = 10
    total = len(tasks)

    for i in range(0, total, chunk_size):
        chunk = tasks[i:i+chunk_size]

        for task in chunk:
            target_val = task.target_data.get(target_key_col)

            if target_val:
                logger.info(f"Fetching SIRET: {target_val}")
                result = await client.get_by_siret(str(target_val))

                with Session(engine) as session:
                    t_update = session.get(ReconciliationTask, task.id)
                    if t_update:
                        t_update.candidate_data = result if result else {}
                        session.add(t_update)
                        session.commit()

                await asyncio.sleep(0.2)

    logger.info("API Worker Finished.")

async def verify_api_connectivity():
    """
    Startup check to verify SIRENE API connectivity.
    """
    token = os.getenv("SIRENE_TOKEN")
    if token:
        logger.info("Verifying SIRENE API connectivity...")
        client = SireneClient(token)
        # Check specific SIRET
        siret = "30133105400041"
        try:
            result = await client.get_by_siret(siret)
            if result:
                name = result.get("uniteLegale.denominationUniteLegale")
                if name == "GLOBAL HYGIENE":
                    logger.success("Startup API Verification SUCCESS: Found GLOBAL HYGIENE")
                else:
                    logger.warning(f"Startup API Verification: Found '{name}', expected 'GLOBAL HYGIENE'")
            else:
                logger.error("Startup API Verification FAILED: No result found")
        except Exception as e:
            logger.error(f"Startup API Verification EXCEPTION: {e}")
    else:
        logger.info("Skipping Startup API Verification (SIRENE_TOKEN not set)")
