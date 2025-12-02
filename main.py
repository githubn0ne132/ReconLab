from nicegui import ui, app, events
from app.db import create_db_and_tables, engine
from app.models import Project
from app.duckdb_client import duckdb_client
from app.engine import initialize_tasks_csv, initialize_tasks_api_pre, run_api_worker
# Import new pages
import app.ui_mapping
import app.ui_validation
import app.export # Register export route
from sqlmodel import Session, select
import asyncio
from pathlib import Path
import os

# Initialize DB
create_db_and_tables()

# Store state
class State:
    def __init__(self):
        self.temp_files = {} # To store uploaded file paths temporarily

state = State()

@ui.page('/')
def index():
    ui.label('ReconLab').classes('text-3xl font-bold mb-4')

    # Project List
    with ui.card().classes('w-full'):
        ui.label('Projects').classes('text-xl')

        columns = [
            {'name': 'id', 'label': 'ID', 'field': 'id'},
            {'name': 'name', 'label': 'Name', 'field': 'name'},
            {'name': 'mode', 'label': 'Mode', 'field': 'mode'},
            {'name': 'status', 'label': 'Status', 'field': 'status'},
            {'name': 'actions', 'label': 'Actions', 'field': 'actions'}
        ]

        rows = []
        with Session(engine) as session:
            projects = session.exec(select(Project)).all()
            for p in projects:
                rows.append({
                    'id': p.id,
                    'name': p.name,
                    'mode': p.mode,
                    'status': p.status,
                })

        # Grid/Table
        table = ui.table(columns=columns, rows=rows, row_key='id').classes('w-full')

        # Custom slot for actions (Not fully supported in pure table definition in simple way,
        # usually use add_slot or render differently. Simplified: Just a list for now, click row to open?)
        # Let's use a slot for the 'actions' column if possible or just make rows clickable.

        table.add_slot('body-cell-actions', r'''
            <q-td key="actions" :props="props">
                <q-btn icon="delete" color="negative" flat dense @click="$parent.$emit('delete', props.row)" />
                <q-btn icon="play_arrow" color="primary" flat dense @click="$parent.$emit('resume', props.row)" />
            </q-td>
        ''')

        def handle_delete(e):
            row = e.args[0]
            with Session(engine) as session:
                proj = session.get(Project, row['id'])
                if proj:
                    # Drop DuckDB tables
                    duckdb_client.drop_table(proj.target_table_name)
                    if proj.source_table_name:
                        duckdb_client.drop_table(proj.source_table_name)
                    session.delete(proj)
                    session.commit()
            ui.notify(f"Deleted project {row['id']}")
            ui.navigate.reload()

        def handle_resume(e):
            row = e.args[0]
            # Navigate based on status
            if row['status'] == 'Setup':
                # Should not happen if created correctly, maybe go to mapping
                 ui.navigate.to(f'/mapping/{row["id"]}')
            elif row['status'] == 'Mapping':
                 ui.navigate.to(f'/mapping/{row["id"]}')
            elif row['status'] in ['Processing', 'Validation', 'Completed']:
                 ui.navigate.to(f'/validation/{row["id"]}')

        table.on('delete', handle_delete)
        table.on('resume', handle_resume)

    # New Project Wizard Button
    ui.button('New Project', on_click=lambda: ui.navigate.to('/create')).classes('mt-4')


@ui.page('/create')
def create_project():
    ui.label('Create New Project').classes('text-2xl font-bold mb-4')

    name_input = ui.input('Project Name').classes('w-full mb-2')

    # Step 2: Target
    ui.label('Step 2: Upload Target CSV').classes('text-lg mt-4')
    target_uploader = ui.upload(label="Target CSV", auto_upload=True, on_upload=lambda e: handle_upload(e, 'target')).classes('w-full')

    # Step 3: Source
    ui.label('Step 3: Select Source').classes('text-lg mt-4')
    source_type = ui.radio(['CSV', 'API'], value='CSV').classes('mb-2')

    source_uploader = ui.upload(label="Source CSV", auto_upload=True, on_upload=lambda e: handle_upload(e, 'source')).bind_visibility_from(source_type, 'value', value='CSV').classes('w-full')

    api_token = ui.input('SIRENE API Token (Optional)').bind_visibility_from(source_type, 'value', value='API')

    uploaded_files = {}

    async def handle_upload(e: events.UploadEventArguments, type_: str):
        # Save file to disk
        local_path = Path("data") / e.file.name
        local_path.parent.mkdir(parents=True, exist_ok=True) # Ensure data dir
        await e.file.save(local_path)
        uploaded_files[type_] = local_path.absolute().as_posix()
        ui.notify(f"Uploaded {e.file.name}")

    def create():
        if not name_input.value:
            ui.notify('Name is required', type='warning')
            return
        if 'target' not in uploaded_files:
            ui.notify('Target CSV is required', type='warning')
            return
        if source_type.value == 'CSV' and 'source' not in uploaded_files:
            ui.notify('Source CSV is required', type='warning')
            return

        with Session(engine) as session:
            # Create Project Record
            project = Project(
                name=name_input.value,
                mode=source_type.value,
                status='Mapping',
                mapping_config = {"api_token": api_token.value} if source_type.value == 'API' else {}
            )
            session.add(project)
            session.commit()
            session.refresh(project)

            # Ingest Data
            proj_id = project.id

            # Target
            target_table = f"proj_{proj_id}_target"
            duckdb_client.ingest_csv(target_table, uploaded_files['target'])
            project.target_table_name = target_table

            # Source
            if source_type.value == 'CSV':
                source_table = f"proj_{proj_id}_source"
                duckdb_client.ingest_csv(source_table, uploaded_files['source'])
                project.source_table_name = source_table

            session.add(project)
            session.commit()

            ui.notify('Project Created!')
            ui.navigate.to(f'/mapping/{proj_id}')

    ui.button('Create & Configure', on_click=create).classes('mt-6')

# Start the app
if __name__ in {"__main__", "__mp_main__"}:
    ui.run(title='ReconLab', port=8080, reload=False)
