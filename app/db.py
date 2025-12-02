from sqlmodel import SQLModel, create_engine, Session
from pathlib import Path

# SQLite Database for Metadata
DB_FILE = Path("reconlab.db")
sqlite_url = f"sqlite:///{DB_FILE}"

connect_args = {"check_same_thread": False}
engine = create_engine(sqlite_url, echo=False, connect_args=connect_args)

def create_db_and_tables():
    SQLModel.metadata.create_all(engine)

def get_session():
    with Session(engine) as session:
        yield session
