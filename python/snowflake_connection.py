"""
Snowflake Connection Manager
============================
Handles connection lifecycle, session management, and query execution.
Supports both synchronous operations and async-friendly patterns.
"""

import logging
from contextlib import contextmanager
from typing import Generator, List, Optional, Tuple

import pandas as pd
import snowflake.connector
from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor

from config import SnowflakeConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("snowflake_connection")


class SnowflakeManager:
    """
    Manages Snowflake connections with context manager support.
    
    Usage:
        config = SnowflakeConfig.from_env()
        manager = SnowflakeManager(config)
        
        with manager.connection() as conn:
            df = manager.query_to_dataframe("SELECT * FROM table")
    """
    
    def __init__(self, config: SnowflakeConfig):
        self.config = config
        self._connection: Optional[SnowflakeConnection] = None
        config.validate()
        logger.info(f"Initialized SnowflakeManager for account: {config.account}")
    
    @contextmanager
    def connection(self) -> Generator[SnowflakeConnection, None, None]:
        """Context manager for database connections."""
        conn = None
        try:
            logger.info("Opening Snowflake connection...")
            conn = snowflake.connector.connect(**self.config.to_connection_params())
            self._connection = conn
            yield conn
        except Exception as e:
            logger.error(f"Connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()
                self._connection = None
                logger.info("Snowflake connection closed")
    
    @contextmanager
    def cursor(self) -> Generator[SnowflakeCursor, None, None]:
        """Context manager for cursors within an existing connection."""
        if not self._connection:
            raise RuntimeError("No active connection. Use within connection() context.")
        
        cursor = self._connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()
    
    def execute(self, query: str, params: Optional[dict] = None) -> SnowflakeCursor:
        """Execute a query and return the cursor."""
        if not self._connection:
            raise RuntimeError("No active connection")
        
        cursor = self._connection.cursor()
        try:
            logger.debug(f"Executing query: {query[:100]}...")
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor
        except Exception as e:
            cursor.close()
            logger.error(f"Query execution failed: {e}")
            raise
    
    def execute_many(self, query: str, data: List[Tuple]) -> int:
        """Execute a query with multiple parameter sets (batch insert)."""
        if not self._connection:
            raise RuntimeError("No active connection")
        
        cursor = self._connection.cursor()
        try:
            logger.info(f"Executing batch insert with {len(data)} rows...")
            cursor.executemany(query, data)
            row_count = cursor.rowcount
            logger.info(f"Inserted {row_count} rows")
            return row_count
        finally:
            cursor.close()
    
    def query_to_dataframe(self, query: str, params: Optional[dict] = None) -> pd.DataFrame:
        """Execute a query and return results as a pandas DataFrame."""
        if not self._connection:
            raise RuntimeError("No active connection")
        
        cursor = self._connection.cursor()
        try:
            logger.debug(f"Executing query to DataFrame: {query[:100]}...")
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            # Fetch as DataFrame
            df = cursor.fetch_pandas_all()
            logger.info(f"Query returned {len(df)} rows")
            return df
        finally:
            cursor.close()
    
    def write_dataframe(
        self, 
        df: pd.DataFrame, 
        table_name: str,
        schema: str = "BRONZE",
        overwrite: bool = False,
        auto_create_table: bool = False
    ) -> dict:
        """
        Write a pandas DataFrame to Snowflake table.
        
        Args:
            df: DataFrame to write
            table_name: Target table name
            schema: Target schema
            overwrite: If True, truncate table first
            auto_create_table: If True, create table from DataFrame schema
        
        Returns:
            dict with success status and row counts
        """
        from snowflake.connector.pandas_tools import write_pandas
        
        if not self._connection:
            raise RuntimeError("No active connection")
        
        logger.info(f"Writing {len(df)} rows to {schema}.{table_name}")
        
        success, num_chunks, num_rows, _ = write_pandas(
            conn=self._connection,
            df=df,
            table_name=table_name,
            schema=schema,
            database=self.config.database,
            overwrite=overwrite,
            auto_create_table=auto_create_table,
        )
        
        result = {
            "success": success,
            "chunks": num_chunks,
            "rows_written": num_rows,
            "table": f"{schema}.{table_name}"
        }
        logger.info(f"Write complete: {result}")
        return result
    
    def use_schema(self, schema: str) -> None:
        """Switch to a different schema."""
        self.execute(f"USE SCHEMA {schema}")
        logger.info(f"Switched to schema: {schema}")
    
    def use_warehouse(self, warehouse: str) -> None:
        """Switch to a different warehouse."""
        self.execute(f"USE WAREHOUSE {warehouse}")
        logger.info(f"Switched to warehouse: {warehouse}")
    
    def get_table_row_count(self, table: str) -> int:
        """Get row count for a table."""
        cursor = self.execute(f"SELECT COUNT(*) FROM {table}")
        result = cursor.fetchone()
        cursor.close()
        return result[0] if result else 0
    
    def table_exists(self, table_name: str, schema: str = None) -> bool:
        """Check if a table exists."""
        schema = schema or self.config.schema
        query = """
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        """
        cursor = self.execute(query, {"1": schema, "2": table_name.upper()})
        result = cursor.fetchone()
        cursor.close()
        return result[0] > 0 if result else False


def get_connection_manager() -> SnowflakeManager:
    """Factory function to create a configured SnowflakeManager."""
    config = SnowflakeConfig.from_env()
    return SnowflakeManager(config)


# Quick test when run directly
if __name__ == "__main__":
    print("Testing Snowflake connection...")
    
    try:
        manager = get_connection_manager()
        with manager.connection() as conn:
            cursor = manager.execute("SELECT CURRENT_TIMESTAMP(), CURRENT_USER(), CURRENT_ROLE()")
            result = cursor.fetchone()
            print(f"✅ Connected successfully!")
            print(f"   Timestamp: {result[0]}")
            print(f"   User: {result[1]}")
            print(f"   Role: {result[2]}")
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("\nMake sure you've set the environment variables in .env file")
