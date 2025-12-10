"""
End-to-End Data Pipeline
========================
Demonstrates data ingestion into Bronze layer, with Dynamic Tables
automatically handling Silver/Gold transformations.

This script simulates realistic data streams:
- IoT sensor readings from manufacturing equipment
- Transaction events from point-of-sale systems  
- Customer clickstream/behavior events
"""

import json
import random
import uuid
from datetime import datetime, timedelta
from typing import Generator

import pandas as pd

from config import PipelineConfig, TABLES
from snowflake_connection import get_connection_manager, logger


# ─────────────────────────────────────────────────────────────────────────────
# DATA GENERATORS
# ─────────────────────────────────────────────────────────────────────────────

class SensorDataGenerator:
    """Generates realistic IoT sensor data."""
    
    SENSOR_TYPES = ["TEMPERATURE", "PRESSURE", "HUMIDITY", "VIBRATION"]
    DEVICES = [f"DEVICE_{str(i).zfill(3)}" for i in range(1, 51)]
    
    @classmethod
    def generate_reading(cls) -> dict:
        """Generate a single sensor reading."""
        sensor_type = random.choice(cls.SENSOR_TYPES)
        device_id = random.choice(cls.DEVICES)
        
        # Realistic ranges per sensor type
        value_ranges = {
            "TEMPERATURE": (15, 85, "CELSIUS"),
            "PRESSURE": (100, 500, "PSI"),
            "HUMIDITY": (20, 80, "PERCENT"),
            "VIBRATION": (0, 100, "MM/S"),
        }
        
        min_val, max_val, unit = value_ranges[sensor_type]
        
        # Occasionally inject anomalies (5% chance)
        if random.random() < 0.05:
            reading_value = random.uniform(max_val * 1.5, max_val * 2)
        else:
            reading_value = random.uniform(min_val, max_val)
        
        return {
            "DEVICE_ID": device_id,
            "SENSOR_TYPE": sensor_type,
            "READING_VALUE": round(reading_value, 2),
            "READING_UNIT": unit,
            "READING_TIMESTAMP": datetime.now() - timedelta(seconds=random.randint(0, 3600)),
            "RAW_PAYLOAD": json.dumps({"raw": True, "version": "1.0", "device": device_id}),
            "SOURCE_FILE": f"iot_stream_{datetime.now().strftime('%Y%m%d')}.json"
        }
    
    @classmethod
    def generate_batch(cls, size: int) -> pd.DataFrame:
        """Generate a batch of sensor readings as DataFrame."""
        records = [cls.generate_reading() for _ in range(size)]
        return pd.DataFrame(records)


class TransactionDataGenerator:
    """Generates realistic e-commerce transaction data."""
    
    TRANSACTION_TYPES = ["SALE", "SALE", "SALE", "SALE", "RETURN", "EXCHANGE"]
    PRODUCTS = [f"SKU_{str(i).zfill(4)}" for i in range(1, 201)]
    
    @classmethod
    def generate_transaction(cls) -> dict:
        """Generate a single transaction."""
        txn_type = random.choice(cls.TRANSACTION_TYPES)
        quantity = random.randint(1, 10)
        unit_price = round(random.uniform(10, 500), 2)
        
        return {
            "TRANSACTION_ID": f"TXN_{uuid.uuid4().hex[:16].upper()}",
            "CUSTOMER_ID": random.randint(10001, 10500),
            "PRODUCT_SKU": random.choice(cls.PRODUCTS),
            "QUANTITY": quantity,
            "UNIT_PRICE": unit_price,
            "TRANSACTION_TYPE": txn_type,
            "TRANSACTION_TIME": datetime.now() - timedelta(hours=random.randint(0, 72)),
            "RAW_DATA": json.dumps({
                "source": "POS",
                "store_id": random.randint(1, 50),
                "terminal": random.randint(1, 10)
            })
        }
    
    @classmethod
    def generate_batch(cls, size: int) -> pd.DataFrame:
        """Generate a batch of transactions as DataFrame."""
        records = [cls.generate_transaction() for _ in range(size)]
        return pd.DataFrame(records)


class CustomerEventGenerator:
    """Generates realistic customer behavior/clickstream data."""
    
    EVENT_TYPES = ["PAGE_VIEW", "CLICK", "ADD_TO_CART", "REMOVE_FROM_CART", "PURCHASE", "SEARCH"]
    DEVICES = ["mobile", "desktop", "tablet"]
    PAGES = [f"/page/{i}" for i in range(1, 101)]
    
    @classmethod
    def generate_session_events(cls, session_size: int = None) -> Generator[dict, None, None]:
        """Generate events for a single user session."""
        session_id = f"SESS_{uuid.uuid4().hex[:24]}"
        customer_id = random.randint(10001, 10500)
        device = random.choice(cls.DEVICES)
        session_size = session_size or random.randint(3, 20)
        
        base_time = datetime.now() - timedelta(hours=random.randint(0, 168))
        
        for i in range(session_size):
            # Sessions typically start with page views
            if i == 0:
                event_type = "PAGE_VIEW"
            else:
                event_type = random.choice(cls.EVENT_TYPES)
            
            yield {
                "EVENT_ID": f"EVT_{uuid.uuid4().hex[:24].upper()}",
                "SESSION_ID": session_id,
                "CUSTOMER_ID": customer_id,
                "EVENT_TYPE": event_type,
                "EVENT_PROPERTIES": json.dumps({"device": device, "event_index": i}),
                "PAGE_URL": random.choice(cls.PAGES),
                "USER_AGENT": f"Mozilla/5.0 ({device})",
                "IP_ADDRESS": f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}",
                "EVENT_TIMESTAMP": base_time + timedelta(seconds=i * random.randint(5, 60))
            }
    
    @classmethod
    def generate_batch(cls, num_sessions: int) -> pd.DataFrame:
        """Generate events for multiple sessions."""
        all_events = []
        for _ in range(num_sessions):
            all_events.extend(list(cls.generate_session_events()))
        return pd.DataFrame(all_events)


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE OPERATIONS
# ─────────────────────────────────────────────────────────────────────────────

class DataPipeline:
    """
    Orchestrates data ingestion into the Bronze layer.
    
    Dynamic Tables in Silver/Gold layers automatically refresh
    based on the TARGET_LAG settings when Bronze data changes.
    """
    
    def __init__(self, config: PipelineConfig = None):
        self.config = config or PipelineConfig.from_env()
        self.manager = get_connection_manager()
        logger.info("DataPipeline initialized")
    
    def ingest_sensor_data(self, batch_size: int = None) -> dict:
        """Ingest a batch of sensor readings into Bronze layer."""
        batch_size = batch_size or self.config.sensor_batch_size
        
        logger.info(f"Generating {batch_size} sensor readings...")
        df = SensorDataGenerator.generate_batch(batch_size)
        
        with self.manager.connection():
            self.manager.use_schema("BRONZE")
            
            # Insert using write_pandas
            result = self.manager.write_dataframe(
                df=df,
                table_name="RAW_SENSOR_READINGS",
                schema="BRONZE"
            )
            
            # Get current table stats
            total_rows = self.manager.get_table_row_count(TABLES["raw_sensors"])
            
        return {
            "operation": "ingest_sensor_data",
            "rows_inserted": result["rows_written"],
            "total_table_rows": total_rows,
            "timestamp": datetime.now().isoformat()
        }
    
    def ingest_transactions(self, batch_size: int = None) -> dict:
        """Ingest a batch of transactions into Bronze layer."""
        batch_size = batch_size or self.config.transaction_batch_size
        
        logger.info(f"Generating {batch_size} transactions...")
        df = TransactionDataGenerator.generate_batch(batch_size)
        
        with self.manager.connection():
            self.manager.use_schema("BRONZE")
            
            result = self.manager.write_dataframe(
                df=df,
                table_name="RAW_TRANSACTIONS", 
                schema="BRONZE"
            )
            
            total_rows = self.manager.get_table_row_count(TABLES["raw_transactions"])
            
        return {
            "operation": "ingest_transactions",
            "rows_inserted": result["rows_written"],
            "total_table_rows": total_rows,
            "timestamp": datetime.now().isoformat()
        }
    
    def ingest_customer_events(self, num_sessions: int = None) -> dict:
        """Ingest customer behavior events into Bronze layer."""
        num_sessions = num_sessions or 100
        
        logger.info(f"Generating events for {num_sessions} sessions...")
        df = CustomerEventGenerator.generate_batch(num_sessions)
        
        with self.manager.connection():
            self.manager.use_schema("BRONZE")
            
            result = self.manager.write_dataframe(
                df=df,
                table_name="RAW_CUSTOMER_EVENTS",
                schema="BRONZE"
            )
            
            total_rows = self.manager.get_table_row_count(TABLES["raw_events"])
            
        return {
            "operation": "ingest_customer_events",
            "rows_inserted": result["rows_written"],
            "sessions_created": num_sessions,
            "total_table_rows": total_rows,
            "timestamp": datetime.now().isoformat()
        }
    
    def run_full_ingestion(self) -> dict:
        """Run a full ingestion cycle for all data types."""
        logger.info("=" * 60)
        logger.info("Starting full data ingestion cycle")
        logger.info("=" * 60)
        
        results = {
            "sensor_data": self.ingest_sensor_data(),
            "transactions": self.ingest_transactions(),
            "customer_events": self.ingest_customer_events(),
        }
        
        logger.info("=" * 60)
        logger.info("Ingestion cycle complete")
        logger.info("=" * 60)
        
        return results
    
    def get_pipeline_status(self) -> dict:
        """Get current status of all pipeline tables."""
        with self.manager.connection():
            # Check if database exists
            try:
                self.manager.execute("USE DATABASE DATA_ENGINEERING_DEMO")
            except Exception as e:
                return {
                    "error": "Database DATA_ENGINEERING_DEMO does not exist. Run sql/01_setup_dynamic_tables.sql first.",
                    "checked_at": datetime.now().isoformat()
                }
            
            # Bronze layer stats
            bronze_stats = {}
            for name, table in [
                ("sensors", TABLES["raw_sensors"]),
                ("transactions", TABLES["raw_transactions"]),
                ("events", TABLES["raw_events"]),
            ]:
                try:
                    bronze_stats[name] = self.manager.get_table_row_count(table)
                except:
                    bronze_stats[name] = 0
            
            # Dynamic table status
            try:
                dt_status = self.manager.query_to_dataframe("""
                    SELECT 
                        NAME,
                        SCHEMA_NAME,
                        TARGET_LAG,
                        SCHEDULING_STATE,
                        LAST_COMPLETED_REFRESH
                    FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES())
                    ORDER BY SCHEMA_NAME, NAME
                """)
                dt_list = dt_status.to_dict(orient="records")
            except:
                dt_list = []
            
        return {
            "bronze_layer": bronze_stats,
            "dynamic_tables": dt_list,
            "checked_at": datetime.now().isoformat()
        }


# ─────────────────────────────────────────────────────────────────────────────
# CLI INTERFACE
# ─────────────────────────────────────────────────────────────────────────────

def main():
    """Main entry point for the data pipeline."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Snowflake Data Engineering Pipeline with Dynamic Tables"
    )
    parser.add_argument(
        "--mode",
        choices=["full", "sensors", "transactions", "events", "status"],
        default="full",
        help="Pipeline mode to run"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Override batch size for data generation"
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Run continuously with random intervals"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=60,
        help="Interval in seconds for continuous mode"
    )
    
    args = parser.parse_args()
    
    pipeline = DataPipeline()
    
    if args.mode == "status":
        status = pipeline.get_pipeline_status()
        print("\n" + "=" * 60)
        print("PIPELINE STATUS")
        print("=" * 60)
        
        if "error" in status:
            print(f"\n❌ {status['error']}")
            print()
            return
        
        print(f"\n📦 Bronze Layer Row Counts:")
        for name, count in status["bronze_layer"].items():
            print(f"   {name}: {count:,} rows")
        print(f"\n🔄 Dynamic Tables:")
        if status["dynamic_tables"]:
            for dt in status["dynamic_tables"]:
                print(f"   {dt['SCHEMA_NAME']}.{dt['NAME']}")
                print(f"      Lag: {dt['TARGET_LAG']} | State: {dt['SCHEDULING_STATE']}")
        else:
            print("   No dynamic tables found")
        print()
        return
    
    if args.continuous:
        import time
        print(f"Running in continuous mode (interval: {args.interval}s)")
        print("Press Ctrl+C to stop\n")
        
        try:
            while True:
                if args.mode == "full":
                    pipeline.run_full_ingestion()
                elif args.mode == "sensors":
                    pipeline.ingest_sensor_data(args.batch_size)
                elif args.mode == "transactions":
                    pipeline.ingest_transactions(args.batch_size)
                elif args.mode == "events":
                    pipeline.ingest_customer_events(args.batch_size)
                
                # Random jitter to simulate realistic load
                sleep_time = args.interval + random.randint(-10, 10)
                print(f"\n⏳ Sleeping for {sleep_time}s...\n")
                time.sleep(max(10, sleep_time))
                
        except KeyboardInterrupt:
            print("\n\n🛑 Pipeline stopped by user")
    else:
        # Single run
        if args.mode == "full":
            results = pipeline.run_full_ingestion()
        elif args.mode == "sensors":
            results = pipeline.ingest_sensor_data(args.batch_size)
        elif args.mode == "transactions":
            results = pipeline.ingest_transactions(args.batch_size)
        elif args.mode == "events":
            results = pipeline.ingest_customer_events(args.batch_size)
        
        print("\n" + "=" * 60)
        print("INGESTION RESULTS")
        print("=" * 60)
        print(json.dumps(results, indent=2, default=str))


if __name__ == "__main__":
    main()
