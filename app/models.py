from typing import Optional, List, Dict
from datetime import datetime, timezone
from sqlmodel import SQLModel, Field
from sqlalchemy import JSON, Column

def utc_now():
    return datetime.now(timezone.utc)

class Project(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True)
    created_date: datetime = Field(default_factory=utc_now)
    mode: str  # "CSV" or "API"
    status: str = Field(default="Setup") # Setup, Mapping, Processing, Validation, Completed

    # Configuration
    target_table_name: Optional[str] = None
    source_table_name: Optional[str] = None

    # JSON Blob for storing the Mapping Configuration
    # Structure: {"join_key": {"target": "col", "source": "col"}, "field_map": {"target_col": "source_field"}}
    mapping_config: Dict = Field(default={}, sa_column=Column(JSON(none_as_null=True)))

class ReconciliationTask(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    project_id: int = Field(index=True)

    # Store original row data from Target (JSON)
    target_data: Dict = Field(default={}, sa_column=Column(JSON(none_as_null=True)))

    # Store potential match data from Source (JSON)
    candidate_data: Optional[Dict] = Field(default=None, sa_column=Column(JSON(none_as_null=True)))

    # Validation Status
    status: str = Field(default="Pending") # Pending, Resolved, Skipped

    # User Decision
    decision: Optional[str] = None # "Keep Target", "Accept Source", "Manual Edit"

    # If decision is Manual Edit or Accept Source, store the final values here
    final_data: Optional[Dict] = Field(default=None, sa_column=Column(JSON(none_as_null=True)))
