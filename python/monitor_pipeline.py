"""
Pipeline Monitoring & Observability
====================================
Monitor Dynamic Tables, track data freshness, and alert on issues.
Provides a real-time dashboard view of the data engineering pipeline.
"""

import json
from datetime import datetime
from typing import Optional

import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box

from config import TABLES, SCHEMAS
from snowflake_connection import get_connection_manager, logger


console = Console()


class PipelineMonitor:
    """
    Monitors the health and performance of the data pipeline.
    Tracks Dynamic Table refresh status, data freshness, and anomalies.
    """
    
    def __init__(self):
        self.manager = get_connection_manager()
        logger.info("PipelineMonitor initialized")
    
    def get_dynamic_table_status(self) -> pd.DataFrame:
        """Get status of all Dynamic Tables in the pipeline."""
        query = """
        SHOW DYNAMIC TABLES IN DATABASE DATA_ENGINEERING_DEMO
        """
        
        with self.manager.connection():
            try:
                self.manager.execute("USE DATABASE DATA_ENGINEERING_DEMO")
                df = self.manager.query_to_dataframe(query)
                # Rename columns for consistency
                if not df.empty:
                    df = df.rename(columns={
                        'name': 'table_name',
                        'schema_name': 'schema_name', 
                        'target_lag': 'target_lag',
                        'scheduling_state': 'scheduling_state',
                    })
                return df
            except Exception as e:
                logger.warning(f"Could not get dynamic table status: {e}")
                return pd.DataFrame()
    
    def get_bronze_freshness(self) -> pd.DataFrame:
        """Check data freshness in Bronze layer tables."""
        query = """
        SELECT 
            'RAW_SENSOR_READINGS' AS table_name,
            MAX(INGESTED_AT) AS latest_ingestion,
            MIN(INGESTED_AT) AS earliest_ingestion,
            COUNT(DISTINCT DEVICE_ID) AS unique_devices,
            TIMESTAMPDIFF('minute', MAX(INGESTED_AT), CURRENT_TIMESTAMP()) AS minutes_since_latest
        FROM BRONZE.RAW_SENSOR_READINGS
        
        UNION ALL
        
        SELECT 
            'RAW_TRANSACTIONS',
            MAX(INGESTED_AT),
            MIN(INGESTED_AT),
            COUNT(DISTINCT CUSTOMER_ID),
            TIMESTAMPDIFF('minute', MAX(INGESTED_AT), CURRENT_TIMESTAMP())
        FROM BRONZE.RAW_TRANSACTIONS
        
        UNION ALL
        
        SELECT 
            'RAW_CUSTOMER_EVENTS',
            MAX(INGESTED_AT),
            MIN(INGESTED_AT),
            COUNT(DISTINCT SESSION_ID),
            TIMESTAMPDIFF('minute', MAX(INGESTED_AT), CURRENT_TIMESTAMP())
        FROM BRONZE.RAW_CUSTOMER_EVENTS
        """
        
        with self.manager.connection():
            try:
                self.manager.execute("USE DATABASE DATA_ENGINEERING_DEMO")
                return self.manager.query_to_dataframe(query)
            except Exception as e:
                logger.warning(f"Could not get bronze freshness: {e}")
                return pd.DataFrame()
    
    def get_anomaly_summary(self) -> pd.DataFrame:
        """Get summary of data quality issues detected in Silver layer."""
        query = """
        SELECT 
            'Sensor Anomalies' AS metric,
            COUNT(*) AS total_records,
            SUM(CASE WHEN IS_ANOMALY THEN 1 ELSE 0 END) AS flagged_records,
            ROUND(SUM(CASE WHEN IS_ANOMALY THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS flag_rate_pct
        FROM SILVER.SENSOR_READINGS_CLEANED
        
        UNION ALL
        
        SELECT 
            'Transaction Validation',
            COUNT(*),
            SUM(CASE WHEN VALIDATION_STATUS != 'VALID' THEN 1 ELSE 0 END),
            ROUND(SUM(CASE WHEN VALIDATION_STATUS != 'VALID' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2)
        FROM SILVER.TRANSACTIONS_ENRICHED
        """
        
        with self.manager.connection():
            try:
                self.manager.execute("USE DATABASE DATA_ENGINEERING_DEMO")
                return self.manager.query_to_dataframe(query)
            except Exception as e:
                logger.warning(f"Could not get anomaly summary: {e}")
                return pd.DataFrame()
    
    def get_gold_metrics(self) -> dict:
        """Get key metrics from Gold layer aggregations."""
        queries = {
            "device_health": """
                SELECT 
                    COUNT(DISTINCT DEVICE_ID) AS devices,
                    SUM(READING_COUNT) AS total_readings,
                    AVG(ANOMALY_RATE_PCT) AS avg_anomaly_rate
                FROM GOLD.DEVICE_HEALTH_HOURLY
                WHERE READING_DATE >= CURRENT_DATE() - 7
            """,
            "sales": """
                SELECT 
                    SUM(NET_SALES) AS total_net_sales,
                    SUM(TOTAL_TRANSACTIONS) AS total_transactions,
                    AVG(AVG_TRANSACTION_VALUE) AS avg_txn_value
                FROM GOLD.DAILY_SALES_SUMMARY
                WHERE TRANSACTION_DATE >= CURRENT_DATE() - 7
            """,
            "customers": """
                SELECT 
                    COUNT(*) AS total_customers,
                    AVG(CONVERSION_RATE) AS avg_conversion_rate,
                    AVG(AVG_SESSION_DURATION) AS avg_session_duration
                FROM GOLD.CUSTOMER_BEHAVIOR_METRICS
            """
        }
        
        results = {}
        with self.manager.connection():
            try:
                self.manager.execute("USE DATABASE DATA_ENGINEERING_DEMO")
            except:
                return {"error": "Database not found"}
            
            for name, query in queries.items():
                try:
                    df = self.manager.query_to_dataframe(query)
                    results[name] = df.to_dict(orient="records")[0] if len(df) > 0 else {}
                except Exception as e:
                    results[name] = {"error": str(e)}
        
        return results
    
    def get_refresh_history(self, limit: int = 20) -> pd.DataFrame:
        """Get recent Dynamic Table refresh history."""
        query = f"""
        SELECT 
            NAME AS table_name,
            STATE,
            STATE_MESSAGE,
            QUERY_ID,
            DATA_TIMESTAMP,
            REFRESH_START_TIME,
            REFRESH_END_TIME,
            TIMESTAMPDIFF('second', REFRESH_START_TIME, REFRESH_END_TIME) AS duration_seconds
        FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
            NAME_PREFIX => 'DATA_ENGINEERING_DEMO'
        ))
        ORDER BY REFRESH_END_TIME DESC
        LIMIT {limit}
        """
        
        with self.manager.connection():
            return self.manager.query_to_dataframe(query)
    
    def check_health(self) -> dict:
        """
        Comprehensive health check of the pipeline.
        Returns status and any alerts.
        """
        alerts = []
        status = "HEALTHY"
        
        # Check Dynamic Table status
        dt_status = self.get_dynamic_table_status()
        if not dt_status.empty:
            for _, row in dt_status.iterrows():
                sched_state = row.get("scheduling_state", "")
                if sched_state and sched_state != "RUNNING":
                    alerts.append(f"⚠️ {row.get('table_name', 'unknown')} scheduling state: {sched_state}")
                    status = "WARNING"
        
        # Check Bronze freshness
        bronze = self.get_bronze_freshness()
        if not bronze.empty:
            for _, row in bronze.iterrows():
                mins = row.get("minutes_since_latest")
                if mins and mins > 60:
                    alerts.append(f"📦 {row['table_name']} no new data for {mins} minutes")
        
        # Check anomaly rates
        try:
            anomalies = self.get_anomaly_summary()
            if not anomalies.empty:
                for _, row in anomalies.iterrows():
                    rate = row.get("flag_rate_pct")
                    if rate and rate > 10:
                        alerts.append(f"🔍 {row['metric']} high flag rate: {rate}%")
                        status = "WARNING"
        except:
            pass  # Anomaly tables may not exist
        
        return {
            "status": status,
            "alerts": alerts,
            "checked_at": datetime.now().isoformat(),
            "dynamic_tables_count": len(dt_status),
            "bronze_tables_count": len(bronze)
        }


def display_dashboard(monitor: PipelineMonitor):
    """Display a rich dashboard of pipeline status."""
    
    console.clear()
    console.print(Panel.fit(
        "[bold cyan]🔄 Snowflake Dynamic Tables Pipeline Monitor[/bold cyan]",
        border_style="cyan"
    ))
    
    # Health Check
    health = monitor.check_health()
    status_color = "green" if health["status"] == "HEALTHY" else "yellow"
    console.print(f"\n[bold]Pipeline Status:[/bold] [{status_color}]{health['status']}[/{status_color}]")
    
    if health["alerts"]:
        console.print("\n[bold yellow]Alerts:[/bold yellow]")
        for alert in health["alerts"]:
            console.print(f"  {alert}")
    
    # Dynamic Tables Status
    console.print("\n")
    dt_table = Table(title="🔄 Dynamic Tables Status", box=box.ROUNDED)
    dt_table.add_column("Schema", style="cyan")
    dt_table.add_column("Table", style="white")
    dt_table.add_column("Target Lag", style="dim")
    dt_table.add_column("State", style="green")
    dt_table.add_column("Last Refresh", style="yellow")
    
    dt_status = monitor.get_dynamic_table_status()
    if dt_status.empty:
        dt_table.add_row("N/A", "No dynamic tables found", "", "", "")
    else:
        for _, row in dt_status.iterrows():
            sched_state = row.get("scheduling_state", "N/A")
            state_style = "green" if sched_state == "RUNNING" else "red"
            dt_table.add_row(
                str(row.get("schema_name", "N/A")),
                str(row.get("table_name", "N/A")),
                str(row.get("target_lag", "N/A")),
                f"[{state_style}]{sched_state}[/{state_style}]",
                "N/A"
            )
    
    console.print(dt_table)
    
    # Bronze Layer Freshness
    console.print("\n")
    bronze_table = Table(title="📦 Bronze Layer Freshness", box=box.ROUNDED)
    bronze_table.add_column("Table", style="cyan")
    bronze_table.add_column("Latest Ingestion", style="yellow")
    bronze_table.add_column("Minutes Since Latest", justify="right")
    bronze_table.add_column("Unique Entities", justify="right", style="dim")
    
    bronze = monitor.get_bronze_freshness()
    if bronze.empty:
        bronze_table.add_row("N/A", "No data", "N/A", "0")
    else:
        for _, row in bronze.iterrows():
            mins = row.get("minutes_since_latest", 0) or 0
            freshness_style = "green" if mins < 30 else "yellow"
            bronze_table.add_row(
                str(row.get("table_name", "N/A")),
                str(row.get("latest_ingestion", "N/A"))[:19] if row.get("latest_ingestion") else "N/A",
                f"[{freshness_style}]{mins or 'N/A'}[/{freshness_style}]",
                f"{row.get('unique_devices', 0):,}"
            )
    
    console.print(bronze_table)
    
    # Data Quality Summary
    console.print("\n")
    quality_table = Table(title="🔍 Data Quality Summary", box=box.ROUNDED)
    quality_table.add_column("Metric", style="cyan")
    quality_table.add_column("Total Records", justify="right", style="white")
    quality_table.add_column("Flagged", justify="right", style="yellow")
    quality_table.add_column("Flag Rate %", justify="right")
    
    try:
        anomalies = monitor.get_anomaly_summary()
        if anomalies.empty:
            quality_table.add_row("N/A", "0", "0", "0%")
        else:
            for _, row in anomalies.iterrows():
                rate = row.get("flag_rate_pct", 0) or 0
                rate_style = "green" if rate < 5 else "yellow" if rate < 10 else "red"
                quality_table.add_row(
                    str(row.get("metric", "N/A")),
                    f"{row.get('total_records', 0):,}",
                    f"{row.get('flagged_records', 0):,}",
                    f"[{rate_style}]{rate:.2f}%[/{rate_style}]"
                )
    except:
        quality_table.add_row("N/A", "Tables not set up", "0", "0%")
    
    console.print(quality_table)
    
    # Gold Layer Metrics
    console.print("\n")
    gold_metrics = monitor.get_gold_metrics()
    
    metrics_table = Table(title="⭐ Gold Layer Highlights (Last 7 Days)", box=box.ROUNDED)
    metrics_table.add_column("Category", style="cyan")
    metrics_table.add_column("Metric", style="white")
    metrics_table.add_column("Value", justify="right", style="green")
    
    if "device_health" in gold_metrics and "error" not in gold_metrics["device_health"]:
        dh = gold_metrics["device_health"]
        metrics_table.add_row("Device Health", "Active Devices", f"{dh.get('devices', 0):,}")
        metrics_table.add_row("Device Health", "Total Readings", f"{dh.get('total_readings', 0):,}")
        metrics_table.add_row("Device Health", "Avg Anomaly Rate", f"{dh.get('avg_anomaly_rate', 0):.2f}%")
    
    if "sales" in gold_metrics and "error" not in gold_metrics["sales"]:
        s = gold_metrics["sales"]
        metrics_table.add_row("Sales", "Net Sales", f"${s.get('total_net_sales', 0):,.2f}")
        metrics_table.add_row("Sales", "Transactions", f"{s.get('total_transactions', 0):,}")
        metrics_table.add_row("Sales", "Avg Transaction", f"${s.get('avg_txn_value', 0):,.2f}")
    
    if "customers" in gold_metrics and "error" not in gold_metrics["customers"]:
        c = gold_metrics["customers"]
        metrics_table.add_row("Customers", "Total Tracked", f"{c.get('total_customers', 0):,}")
        metrics_table.add_row("Customers", "Avg Conversion", f"{c.get('avg_conversion_rate', 0):.2f}%")
        metrics_table.add_row("Customers", "Avg Session (sec)", f"{c.get('avg_session_duration', 0):,.0f}")
    
    console.print(metrics_table)
    
    console.print(f"\n[dim]Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}[/dim]")


def main():
    """Main entry point for the monitoring dashboard."""
    import argparse
    import time
    
    parser = argparse.ArgumentParser(description="Pipeline Monitoring Dashboard")
    parser.add_argument(
        "--mode",
        choices=["dashboard", "health", "history", "json"],
        default="dashboard",
        help="Output mode"
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Continuously refresh the dashboard"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=30,
        help="Refresh interval in seconds for watch mode"
    )
    
    args = parser.parse_args()
    
    monitor = PipelineMonitor()
    
    if args.mode == "health":
        health = monitor.check_health()
        print(json.dumps(health, indent=2))
    
    elif args.mode == "history":
        history = monitor.get_refresh_history()
        print(history.to_string())
    
    elif args.mode == "json":
        data = {
            "health": monitor.check_health(),
            "dynamic_tables": monitor.get_dynamic_table_status().to_dict(orient="records"),
            "bronze_freshness": monitor.get_bronze_freshness().to_dict(orient="records"),
            "anomalies": monitor.get_anomaly_summary().to_dict(orient="records"),
            "gold_metrics": monitor.get_gold_metrics()
        }
        print(json.dumps(data, indent=2, default=str))
    
    elif args.mode == "dashboard":
        if args.watch:
            try:
                while True:
                    display_dashboard(monitor)
                    console.print(f"\n[dim]Refreshing in {args.interval}s... (Ctrl+C to exit)[/dim]")
                    time.sleep(args.interval)
            except KeyboardInterrupt:
                console.print("\n[bold]Monitoring stopped.[/bold]")
        else:
            display_dashboard(monitor)


if __name__ == "__main__":
    main()
