from nicegui import ui
from app.db import engine
from app.models import Project, ReconciliationTask
from sqlmodel import Session, select
from app.engine import run_api_worker
import asyncio
from loguru import logger

@ui.page('/validation/{project_id}')
def validation_page(project_id: int):
    # Check Project
    with Session(engine) as session:
        project = session.get(Project, project_id)
        if not project:
            ui.label('Project not found')
            return

    # Header
    ui.label(f'Validation: {project.name}').classes('text-2xl font-bold mb-4')

    # Progress Bar / Stats
    # Note: We need reactive state for this.
    stats_label = ui.label('Loading stats...')

    # If API mode, ensure worker is running?
    if project.mode == 'API':
        # Simple check: Trigger worker once.
        # In a real app, this should be a robust background service.
        # Here we kick it off.
        token = project.mapping_config.get("api_token")
        asyncio.create_task(run_api_worker(project_id, token))

    # Task Container (The Card)
    card_container = ui.column().classes('w-full')

    def load_next_task():
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

    def render_task_card(task: ReconciliationTask):
        with card_container:
            with ui.card().classes('w-full'):
                ui.label(f'Task ID: {task.id}').classes('text-xs text-gray-400')

                # Check if API is still loading (candidate is None and API mode)
                if project.mode == 'API' and task.candidate_data is None:
                    ui.spinner('dots', size='lg')
                    ui.label('Fetching from API...')
                    ui.timer(2.0, load_next_task, once=True) # Poll
                    return

                # Comparison Grid
                with ui.grid(columns=2).classes('w-full gap-4'):
                    # Target (Left)
                    with ui.column():
                        ui.label('TARGET (Original)').classes('font-bold border-b w-full')
                        render_data_fields(task.target_data, task.candidate_data, "target")

                    # Source (Right)
                    with ui.column():
                        ui.label('SOURCE (Candidate)').classes('font-bold border-b w-full')
                        if task.candidate_data:
                            render_data_fields(task.candidate_data, task.target_data, "source", project.mapping_config.get('field_map'))
                        else:
                            ui.label('No Match Found').classes('text-red-500 italic')

                # Actions
                with ui.row().classes('mt-4 justify-center gap-4'):
                    ui.button('Keep Target', color='grey', on_click=lambda: submit_decision(task.id, 'Keep Target'))

                    if task.candidate_data:
                         ui.button('Accept Source', color='green', on_click=lambda: submit_decision(task.id, 'Accept Source'))

                    ui.button('Manual Edit', color='blue', on_click=lambda: open_edit_dialog(task))

    def open_edit_dialog(task: ReconciliationTask):
        with ui.dialog() as dialog, ui.card().classes('w-96'):
            ui.label(f'Manual Edit (ID: {task.id})').classes('text-lg font-bold mb-2')

            # Form container
            edits = {}
            scroll_area = ui.scroll_area().classes('h-64 border p-2 mb-4')

            with scroll_area:
                # Pre-fill with Target Data
                for k, v in task.target_data.items():
                    # We store the inputs in a dict
                    edits[k] = ui.input(label=k, value=str(v)).classes('w-full')

            def save_edits():
                # Collect values
                final_values = {k: inp.value for k, inp in edits.items()}

                # Save
                with Session(engine) as session:
                    t = session.get(ReconciliationTask, task.id)
                    if t:
                        t.final_data = final_values
                        t.decision = 'Manual Edit'
                        t.status = 'Resolved'
                        session.add(t)
                        session.commit()

                dialog.close()
                load_next_task()

            with ui.row().classes('w-full justify-end'):
                ui.button('Cancel', color='grey', on_click=dialog.close)
                ui.button('Save', color='blue', on_click=save_edits)

        dialog.open()

    def render_data_fields(data, other_data, side, field_map=None):
        # We need to know which fields to compare.
        # If Target, show all keys.
        # If Source, show keys that map to Target, or all?
        # Let's show all keys in data for now, highlighting matches.

        # Better approach based on requirement: "Diffing: Fields with differences must be visually highlighted"
        # We rely on Field Map to know what compares to what.

        map_rev = {v: k for k, v in field_map.items()} if field_map else {}

        for k, v in data.items():
            # Check for difference
            is_diff = False
            bg_class = ""

            if side == "target":
                # Is there a mapped source field?
                mapped_src_field = field_map.get(k) if field_map else None
                if mapped_src_field and other_data:
                    src_val = other_data.get(mapped_src_field)
                    if str(src_val) != str(v):
                        is_diff = True
                        bg_class = "bg-red-100"

            elif side == "source":
                 # Is this field mapped to a target field?
                 mapped_target_field = map_rev.get(k)
                 if mapped_target_field and other_data:
                     target_val = other_data.get(mapped_target_field)
                     if str(target_val) != str(v):
                         is_diff = True
                         bg_class = "bg-green-100" # Green for potential new value

            with ui.row().classes(f'w-full justify-between {bg_class} p-1'):
                ui.label(k).classes('font-semibold text-xs')
                ui.label(str(v)).classes('text-sm truncate')

    def submit_decision(task_id, decision):
        with Session(engine) as session:
            t = session.get(ReconciliationTask, task_id)
            if t:
                t.decision = decision
                t.status = 'Resolved'

                # Logic for Final Data
                if decision == 'Keep Target':
                    t.final_data = t.target_data
                elif decision == 'Accept Source':
                    # Merge Source into Target based on map
                    final = t.target_data.copy()
                    map_cfg = project.mapping_config.get('field_map', {})
                    src = t.candidate_data or {}

                    for target_col, source_field in map_cfg.items():
                        if source_field in src:
                            final[target_col] = src[source_field]
                    t.final_data = final

                session.add(t)
                session.commit()

        load_next_task()

    # Initial Load
    load_next_task()
