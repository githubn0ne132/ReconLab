from nicegui import app
from app.db import engine
from app.models import Project, ReconciliationTask
from sqlmodel import Session, select
from fastapi import Response
import csv
import io

@app.get('/export/{project_id}')
def export_project(project_id: int):
    with Session(engine) as session:
        project = session.get(Project, project_id)
        if not project:
            return Response("Project not found", status_code=404)

        # Fetch all tasks (Resolved)
        # Ideally we export all, or just resolved? Usually "The Result".
        # Let's export all, with status.
        tasks = session.exec(select(ReconciliationTask).where(ReconciliationTask.project_id == project_id)).all()

        if not tasks:
            return Response("No data to export", status_code=404)

        # Determine Columns from the first task's target data (or final data)
        # We need the original structure.
        # Use target_data keys from first task.
        first_task = tasks[0]
        field_names = list(first_task.target_data.keys())

        # Add recon status
        field_names.append("_recon_status")

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=field_names)
        writer.writeheader()

        for task in tasks:
            # Determine row data
            row_data = {}
            if task.final_data:
                row_data = task.final_data.copy()
            else:
                row_data = task.target_data.copy()

            # Add Status
            # e.g. "Modified" if Final != Target, or based on Decision
            status_val = "Pending"
            if task.status == 'Resolved':
                if task.decision == 'Keep Target':
                    status_val = "Original"
                elif task.decision == 'Accept Source':
                    status_val = "Modified"
                elif task.decision == 'Manual Edit':
                    status_val = "Modified"

            row_data["_recon_status"] = status_val

            # Ensure only relevant fields are written
            # (In case final_data has extra fields?)
            clean_row = {k: v for k, v in row_data.items() if k in field_names}

            writer.writerow(clean_row)

        return Response(output.getvalue(), media_type="text/csv")
