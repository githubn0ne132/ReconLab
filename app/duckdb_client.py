import duckdb
from pathlib import Path
from loguru import logger
from typing import Optional, List, Dict, Any
import os

DUCKDB_FILE = Path(os.getenv("DUCKDB_FILE", "reconlab.duckdb"))

class DuckDBClient:
    def __init__(self):
        self.conn = duckdb.connect(str(DUCKDB_FILE))
        logger.info(f"Connected to DuckDB at {DUCKDB_FILE}")

    def ingest_csv(self, table_name: str, csv_path: str):
        """
        Ingests a CSV file into a DuckDB table using read_csv_auto.
        """
        try:
            # Drop table if exists
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")

            # Create table and insert data
            query = f"""
            CREATE TABLE {table_name} AS
            SELECT * FROM read_csv_auto('{csv_path}', normalize_names=True)
            """
            self.conn.execute(query)
            logger.info(f"Successfully ingested {csv_path} into {table_name}")

            # Return column names
            return self.get_columns(table_name)
        except Exception as e:
            logger.error(f"Failed to ingest CSV: {e}")
            raise e

    def get_columns(self, table_name: str) -> List[str]:
        try:
            # query = f"PRAGMA table_info('{table_name}')"
            # result = self.conn.execute(query).fetchall()
            # return [row[1] for row in result]

            # Using DESCRIBE is also possible, or LIMIT 0
            df = self.conn.execute(f"SELECT * FROM {table_name} LIMIT 0").df()
            return list(df.columns)
        except Exception as e:
             logger.error(f"Failed to get columns for {table_name}: {e}")
             return []

    def drop_table(self, table_name: str):
        try:
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            logger.info(f"Dropped table {table_name}")
        except Exception as e:
            logger.error(f"Failed to drop table {table_name}: {e}")

    def query(self, query: str, params: Optional[List[Any]] = None) -> List[Any]:
        """
        Executes a raw query and returns the result.
        """
        try:
            if params:
                return self.conn.execute(query, params).fetchall()
            else:
                return self.conn.execute(query).fetchall()
        except Exception as e:
            logger.error(f"Query failed: {query} Error: {e}")
            raise e

    def query_as_dict(self, query: str, params: Optional[List[Any]] = None) -> List[Dict]:
        """
        Executes a query and returns list of dicts.
        """
        try:
            if params:
                df = self.conn.execute(query, params).df()
            else:
                df = self.conn.execute(query).df()

            # Convert NaN to None for JSON compatibility if needed,
            # though SQLModel/JSON handling might prefer native types.
            # For now, just to_dict
            return df.to_dict(orient='records')
        except Exception as e:
            logger.error(f"Query failed: {query} Error: {e}")
            raise e

# Global instance
duckdb_client = DuckDBClient()
