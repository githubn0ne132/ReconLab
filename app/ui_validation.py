from nicegui import ui
from app.db import engine
from app.models import Project, ReconciliationTask
from sqlmodel import Session, select
from app.engine import run_api_worker
import asyncio
from typing import Dict, Any, Optional, List
from loguru import logger

@ui.page('/validation/{project_id}')
def validation_page(project_id: int) -> None:
    # Check Project
    with Session(engine) as session:
        project = session.get(Project, project_id)
        if not project:
            ui.label('Project not found')
            return

    # Header
    ui.label(f'Validation: {project.name}').classes('text-2xl font-bold mb-4')

    # Progress Bar / Stats
    stats_label = ui.label('Loading stats...')

    # If API mode, ensure worker is running?
    if project.mode == 'API':
        token = project.mapping_config.get("api_token")
        asyncio.create_task(run_api_worker(project_id, token))

    # Task Container (The Card)
    card_container = ui.column().classes('w-full')

    def load_next_task() -> None:
        card_container.clear()

        with Session(engine) as session:
            # Fetch pending task
            # Prioritize "Pending"
            statement = select(ReconciliationTask).where(
                ReconciliationTask.project_id == project_id,
                ReconciliationTask.status == 'Pending'
            ).limit(1)
            task = session.exec(statement).first()

            # Update Stats
            total = session.query(ReconciliationTask).filter(ReconciliationTask.project_id == project_id).count()
            pending = session.query(ReconciliationTask).filter(ReconciliationTask.project_id == project_id, ReconciliationTask.status == 'Pending').count()
            stats_label.set_text(f"Progress: {total - pending}/{total} Validated")

            if not task:
                with card_container:
                    ui.label('All tasks completed!').classes('text-xl text-green-500')
                    ui.button('Export Results', on_click=lambda: ui.download(f'/export/{project_id}', filename=f'{project.name}_export.csv'))
                return

            # Render Card
            render_task_card(task)

    def render_task_card(task: ReconciliationTask) -> None:
        with card_container:
            with ui.card().classes('w-full'):
                ui.label(f'Task ID: {task.id}').classes('text-xs text-gray-400')

                # Check if API is still loading (candidate is None and API mode)
                if project.mode == 'API' and task.candidate_data is None:
                    ui.spinner('dots', size='lg')
                    ui.label('Fetching from API...')
                    ui.timer(2.0, load_next_task, once=True) # Poll
                    return

                # Get Field Map
                field_map = project.mapping_config.get('field_map', {})

                # Container for Golden Inputs
                golden_inputs: Dict[str, ui.input] = {}

                def set_val(key: str, val: Any) -> None:
                    if key in golden_inputs:
                        golden_inputs[key].value = str(val)

                def apply_all(source: str) -> None:
                    for key, target_val in task.target_data.items():
                        if source == 'A':
                            set_val(key, target_val)
                        elif source == 'B':
                            source_field = field_map.get(key)
                            if source_field and task.candidate_data:
                                val = task.candidate_data.get(source_field)
                                if val is not None:
                                    set_val(key, val)

                # Grid Layout (4 Columns)
                with ui.grid(columns=4).classes('w-full gap-4 items-center'):
                    # Headers
                    ui.label('Field').classes('font-bold border-b')
                    ui.label('Target (A)').classes('font-bold border-b')
                    ui.label('Source (B)').classes('font-bold border-b')
                    ui.label('Golden (C)').classes('font-bold border-b')

                    # Iterate Target Keys
                    for key, target_val in task.target_data.items():
                        # 1. Field Name
                        ui.label(key).classes('text-sm font-semibold')

                        # 2. Target Value (A)
                        t_val_str = str(target_val)
                        ui.label(t_val_str).classes('cursor-pointer hover:text-blue-600 p-1 rounded hover:bg-gray-100').on('click', lambda k=key, v=target_val: set_val(k, v)).tooltip('Click to copy to Golden')

                        # 3. Source Value (B)
                        source_field = field_map.get(key)
                        source_val = None
                        bg_class = ""

                        if source_field and task.candidate_data:
                            source_val = task.candidate_data.get(source_field)
                            # Highlight diff
                            if str(source_val) != t_val_str:
                                bg_class = "text-orange-600 font-medium"

                        s_val_str = str(source_val) if source_val is not None else "-"

                        lbl = ui.label(s_val_str).classes(f'cursor-pointer hover:text-blue-600 p-1 rounded hover:bg-gray-100 {bg_class}').tooltip('Click to copy to Golden')
                        if source_val is not None:
                            lbl.on('click', lambda k=key, v=source_val: set_val(k, v))

                        # 4. Golden Value (C)
                        # Initialize with Target Value
                        inp = ui.input(value=t_val_str).classes('w-full')
                        golden_inputs[key] = inp

                # Actions
                ui.separator().classes('my-4')
                with ui.row().classes('w-full justify-between'):
                    with ui.row():
                        ui.button('Keep All A', on_click=lambda: apply_all('A')).classes('mr-2')
                        ui.button('Keep All B', on_click=lambda: apply_all('B'))

                    ui.button('Confirm & Save', on_click=lambda: save_task(task.id, golden_inputs)).classes('bg-green-500 text-white')

    def save_task(task_id: int, inputs: Dict[str, ui.input]) -> None:
        final_data = {k: inp.value for k, inp in inputs.items()}
        submit_decision(task_id, final_data)

    def submit_decision(task_id: int, final_data: Dict[str, Any]) -> None:
        with Session(engine) as session:
            t = session.get(ReconciliationTask, task_id)
            if t:
                t.decision = 'User Confirmed' # Generic decision label
                t.status = 'Resolved'
                t.final_data = final_data
                session.add(t)
                session.commit()

        load_next_task()

    # Initial Load
    load_next_task()
