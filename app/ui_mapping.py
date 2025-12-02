from nicegui import ui
from app.db import engine
from app.models import Project
from app.duckdb_client import duckdb_client
from app.sirene import SireneClient
from app.engine import initialize_tasks_csv, initialize_tasks_api_pre
from sqlmodel import Session
from loguru import logger
from typing import List, Dict, Optional, Any
import asyncio

@ui.page('/mapping/{project_id}')
def mapping_page(project_id: int) -> None:
    with Session(engine) as session:
        project = session.get(Project, project_id)
        if not project:
            ui.label('Project not found')
            return

    ui.label(f'Mapping Configuration: {project.name}').classes('text-2xl font-bold mb-4')

    # Fetch Columns
    target_cols = duckdb_client.get_columns(project.target_table_name)
    source_cols = []

    if project.mode == 'CSV':
        source_cols = duckdb_client.get_columns(project.source_table_name)
    else:
        # API Mode - Use flattened SIRENE fields
        # Ideally we fetch these from the client helper
        client = SireneClient()
        source_cols = client.get_common_fields()
        # Allow user to type custom if needed? For now strict list + maybe "Other"

    # State for selections
    selections: Dict[str, Any] = {
        'join_target': None,
        'join_source': None,
        'field_map': [] # List of {target, source}
    }

    # Step 1: Join Key
    with ui.card().classes('w-full mb-4'):
        ui.label('Step 1: Define Join Key').classes('text-xl')
        ui.label('Select the columns used to match records.').classes('text-gray-500 text-sm')

        with ui.row():
            ui.select(target_cols, label='Target Column (ID/Key)', on_change=lambda e: update_selection('join_target', e.value)).classes('w-64')

            label_src = 'Source Column' if project.mode == 'CSV' else 'API Query ID (Usually same as Target)'
            # If API, we actually just need to know which Target column holds the SIRET.
            # So "Source Column" selection is irrelevant unless we are mapping output?
            # Prompt says: "API Mode: Select which Target Column acts as the query ID."

            if project.mode == 'CSV':
                ui.select(source_cols, label=label_src, on_change=lambda e: update_selection('join_source', e.value)).classes('w-64')
            else:
                ui.label('Using Target Column as SIRET Query ID').classes('mt-4 ml-4')
                # Implicitly, the join source is the API query result, but we don't map it here.
                # We just need to know the target column.


    # Step 2: Field Map
    with ui.card().classes('w-full mb-4'):
        ui.label('Step 2: Map Fields').classes('text-xl')
        ui.label('Map Source fields to Target columns for reconciliation/overwrite.').classes('text-gray-500 text-sm')

        container = ui.column().classes('w-full')

        def add_mapping_row():
            with container:
                with ui.row().classes('items-center'):
                    t_sel = ui.select(target_cols, label='Target Column').classes('w-48')
                    ui.icon('arrow_right')
                    s_sel = ui.select(source_cols, label='Source Field').classes('w-64') # Source fields can be many

                    # Track this row
                    selections['field_map'].append({'target_ui': t_sel, 'source_ui': s_sel})

        ui.button('Add Field Mapping', on_click=add_mapping_row).classes('mt-2')

    def update_selection(key: str, value: Any) -> None:
        selections[key] = value

    async def finish_setup() -> None:
        # Validate
        if not selections['join_target']:
            ui.notify('Please select a Target Join Key', type='warning')
            return
        if project.mode == 'CSV' and not selections['join_source']:
            ui.notify('Please select a Source Join Key', type='warning')
            return

        # Build Config
        mapping_config = project.mapping_config or {}

        if project.mode == 'CSV':
            mapping_config['join_key'] = {
                'target': selections['join_target'],
                'source': selections['join_source']
            }
        else:
            mapping_config['join_key'] = {
                'target': selections['join_target']
            }

        # Field Map
        field_map = {}
        for item in selections['field_map']:
            t = item['target_ui'].value
            s = item['source_ui'].value
            if t and s:
                field_map[t] = s
        mapping_config['field_map'] = field_map

        # Save to DB
        with Session(engine) as session:
            # Re-fetch project to ensure attachment
            p = session.get(Project, project_id)
            p.mapping_config = mapping_config
            p.status = 'Processing'
            session.add(p)
            session.commit()

        # Trigger Engine (Async to avoid blocking UI)
        ui.notify('Processing data... Please wait.', type='info', timeout=None)

        if project.mode == 'CSV':
            await asyncio.to_thread(initialize_tasks_csv, project_id)
        else:
            await asyncio.to_thread(initialize_tasks_api_pre, project_id)

        ui.notify('Processing Complete!')
        ui.navigate.to(f'/validation/{project_id}')

    ui.button('Finish & Start Processing', on_click=finish_setup).classes('bg-green-500 text-white mt-4')
