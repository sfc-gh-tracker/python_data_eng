"""
Snowflake Configuration Management
==================================
Centralized configuration for Snowflake connections and pipeline settings.
Uses environment variables for security - never commit credentials!
"""

import os
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

# Load .env file from parent directory
from dotenv import load_dotenv
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)


@dataclass
class SnowflakeConfig:
    """Snowflake connection configuration."""
    account: str
    user: str
    password: str
    warehouse: str = "WH_DATA_ENG"
    database: str = "DATA_ENGINEERING_DEMO"
    schema: str = "BRONZE"
    role: str = "ACCOUNTADMIN"
    
    # Optional settings
    authenticator: Optional[str] = None  # 'externalbrowser' for SSO
    private_key_path: Optional[str] = None  # For key-pair auth
    
    @classmethod
    def from_env(cls) -> "SnowflakeConfig":
        """Load configuration from environment variables."""
        return cls(
            account=os.environ.get("SNOWFLAKE_ACCOUNT", ""),
            user=os.environ.get("SNOWFLAKE_USER", ""),
            password=os.environ.get("SNOWFLAKE_PASSWORD", ""),
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "WH_DATA_ENG"),
            database=os.environ.get("SNOWFLAKE_DATABASE", "DATA_ENGINEERING_DEMO"),
            schema=os.environ.get("SNOWFLAKE_SCHEMA", "BRONZE"),
            role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
            authenticator=os.environ.get("SNOWFLAKE_AUTHENTICATOR"),
            private_key_path=os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH"),
        )
    
    def validate(self) -> bool:
        """Validate required configuration is present."""
        required = ["account", "user"]
        missing = [f for f in required if not getattr(self, f)]
        
        if not self.password and not self.authenticator and not self.private_key_path:
            missing.append("password/authenticator/private_key_path")
        
        if missing:
            raise ValueError(f"Missing required configuration: {', '.join(missing)}")
        return True
    
    def to_connection_params(self) -> dict:
        """Convert to snowflake-connector-python connection parameters."""
        params = {
            "account": self.account,
            "user": self.user,
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema,
            "role": self.role,
        }
        
        if self.authenticator:
            params["authenticator"] = self.authenticator
        elif self.private_key_path:
            params["private_key_file"] = self.private_key_path
        else:
            params["password"] = self.password
            
        return params


@dataclass
class PipelineConfig:
    """Data pipeline configuration settings."""
    
    # Batch sizes for data generation
    sensor_batch_size: int = 1000
    transaction_batch_size: int = 500
    event_batch_size: int = 2000
    
    # Dynamic table refresh settings
    target_lag_minutes: int = 1
    
    # Data retention (days)
    bronze_retention_days: int = 90
    silver_retention_days: int = 365
    gold_retention_days: int = 730
    
    # Monitoring
    alert_threshold_lag_minutes: int = 10
    
    @classmethod
    def from_env(cls) -> "PipelineConfig":
        """Load pipeline config from environment variables."""
        return cls(
            sensor_batch_size=int(os.environ.get("SENSOR_BATCH_SIZE", "1000")),
            transaction_batch_size=int(os.environ.get("TRANSACTION_BATCH_SIZE", "500")),
            event_batch_size=int(os.environ.get("EVENT_BATCH_SIZE", "2000")),
            target_lag_minutes=int(os.environ.get("TARGET_LAG_MINUTES", "1")),
        )


# Medallion layer schemas
SCHEMAS = {
    "bronze": "BRONZE",
    "silver": "SILVER", 
    "gold": "GOLD",
    "staging": "STAGING",
}

# Table mappings
TABLES = {
    "raw_sensors": f"{SCHEMAS['bronze']}.RAW_SENSOR_READINGS",
    "raw_transactions": f"{SCHEMAS['bronze']}.RAW_TRANSACTIONS",
    "raw_events": f"{SCHEMAS['bronze']}.RAW_CUSTOMER_EVENTS",
    "clean_sensors": f"{SCHEMAS['silver']}.SENSOR_READINGS_CLEANED",
    "enriched_transactions": f"{SCHEMAS['silver']}.TRANSACTIONS_ENRICHED",
    "customer_sessions": f"{SCHEMAS['silver']}.CUSTOMER_SESSIONS",
    "device_health": f"{SCHEMAS['gold']}.DEVICE_HEALTH_HOURLY",
    "daily_sales": f"{SCHEMAS['gold']}.DAILY_SALES_SUMMARY",
    "customer_behavior": f"{SCHEMAS['gold']}.CUSTOMER_BEHAVIOR_METRICS",
    "product_performance": f"{SCHEMAS['gold']}.PRODUCT_PERFORMANCE",
}
