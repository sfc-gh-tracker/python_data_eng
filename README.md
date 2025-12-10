# 🔄 Snowflake Dynamic Tables Demo

## End-to-End Data Engineering with Python

This demo showcases a modern data engineering pipeline using **Snowflake Dynamic Tables** and **Python**. It implements a complete medallion architecture (Bronze → Silver → Gold) with automatic incremental transformations.

---

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DATA SOURCES                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│  IoT Sensors  │  Transaction Systems  │  Clickstream/Events                 │
└───────┬───────┴───────────┬───────────┴───────────┬─────────────────────────┘
        │                   │                       │
        ▼                   ▼                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        🥉 BRONZE LAYER (Raw)                                │
│  ┌──────────────────┐ ┌────────────────────┐ ┌────────────────────────┐    │
│  │ RAW_SENSOR_      │ │ RAW_TRANSACTIONS   │ │ RAW_CUSTOMER_EVENTS    │    │
│  │ READINGS         │ │                    │ │                        │    │
│  └────────┬─────────┘ └─────────┬──────────┘ └───────────┬────────────┘    │
└───────────│─────────────────────│────────────────────────│──────────────────┘
            │                     │                        │
            │    ═══════════════════════════════════════   │
            │         🔄 DYNAMIC TABLES AUTO-REFRESH       │
            │    ═══════════════════════════════════════   │
            ▼                     ▼                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       🥈 SILVER LAYER (Cleaned)                             │
│  ┌──────────────────┐ ┌────────────────────┐ ┌────────────────────────┐    │
│  │ SENSOR_READINGS_ │ │ TRANSACTIONS_      │ │ CUSTOMER_SESSIONS      │    │
│  │ CLEANED          │ │ ENRICHED           │ │                        │    │
│  │ • Anomaly flags  │ │ • Validation       │ │ • Sessionization       │    │
│  │ • Type cleaning  │ │ • Line totals      │ │ • Funnel tracking      │    │
│  └────────┬─────────┘ └─────────┬──────────┘ └───────────┬────────────┘    │
└───────────│─────────────────────│────────────────────────│──────────────────┘
            │                     │                        │
            │    ═══════════════════════════════════════   │
            │         🔄 DYNAMIC TABLES AUTO-REFRESH       │
            │    ═══════════════════════════════════════   │
            ▼                     ▼                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        🥇 GOLD LAYER (Business)                             │
│  ┌──────────────────┐ ┌────────────────────┐ ┌────────────────────────┐    │
│  │ DEVICE_HEALTH_   │ │ DAILY_SALES_       │ │ CUSTOMER_BEHAVIOR_     │    │
│  │ HOURLY           │ │ SUMMARY            │ │ METRICS                │    │
│  ├──────────────────┤ ├────────────────────┤ ├────────────────────────┤    │
│  │ PRODUCT_         │ │                    │ │                        │    │
│  │ PERFORMANCE      │ │                    │ │                        │    │
│  └──────────────────┘ └────────────────────┘ └────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 🚀 Quick Start

### 1. Prerequisites

- Snowflake account with `ACCOUNTADMIN` role (or equivalent privileges)
- Python 3.9+
- pip package manager

### 2. Setup Snowflake Objects

Run the SQL setup script in Snowflake:

```sql
-- Execute in Snowflake Worksheet or SnowSQL
!source sql/01_setup_dynamic_tables.sql
```

Or run step-by-step:
```sql
-- 1. Create database and schemas
CREATE DATABASE IF NOT EXISTS DATA_ENGINEERING_DEMO;
CREATE SCHEMA IF NOT EXISTS DATA_ENGINEERING_DEMO.BRONZE;
CREATE SCHEMA IF NOT EXISTS DATA_ENGINEERING_DEMO.SILVER;
CREATE SCHEMA IF NOT EXISTS DATA_ENGINEERING_DEMO.GOLD;

-- 2. Run the full setup script
-- See sql/01_setup_dynamic_tables.sql
```

### 3. Configure Python Environment

```bash
# Navigate to demo directory
cd python_data_eng

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure environment variables
cp env.sample .env
# Edit .env with your Snowflake credentials
```

### 4. Run the Pipeline

```bash
cd python

# Test connection
python snowflake_connection.py

# Run data ingestion (single batch)
python data_pipeline.py --mode full

# Run specific data type
python data_pipeline.py --mode sensors --batch-size 500

# Run continuously (simulates streaming)
python data_pipeline.py --mode full --continuous --interval 60
```

### 5. Monitor the Pipeline

```bash
# Interactive dashboard
python monitor_pipeline.py

# Watch mode (auto-refresh)
python monitor_pipeline.py --watch --interval 30

# Health check (JSON output)
python monitor_pipeline.py --mode health

# Full status as JSON
python monitor_pipeline.py --mode json
```

---

## 📁 Project Structure

```
python_data_eng/
├── README.md                    # This file
├── requirements.txt             # Python dependencies
├── env.sample                   # Environment template
│
├── sql/
│   └── 01_setup_dynamic_tables.sql   # Full Snowflake setup
│
├── python/
│   ├── config.py                # Configuration management
│   ├── snowflake_connection.py  # Connection utilities
│   ├── data_pipeline.py         # Main ingestion pipeline
│   └── monitor_pipeline.py      # Monitoring dashboard
│
└── notebooks/
    └── 01_pipeline_walkthrough.ipynb  # Interactive tutorial
```

---

## 🔑 Key Concepts

### Dynamic Tables

Dynamic Tables are Snowflake's declarative approach to data transformation:

```sql
CREATE DYNAMIC TABLE SILVER.SENSOR_READINGS_CLEANED
    TARGET_LAG = '1 minute'  -- Auto-refresh to stay within 1 min of source
    WAREHOUSE = WH_DATA_ENG
    AS
SELECT
    -- Transformation logic here
FROM BRONZE.RAW_SENSOR_READINGS;
```

**Benefits:**
- ✅ **Declarative** - Define *what* you want, not *how* to get it
- ✅ **Automatic Refresh** - Snowflake handles incremental updates
- ✅ **Cost Efficient** - Only processes changed data
- ✅ **No Orchestration** - No need for external schedulers

### Medallion Architecture

| Layer | Purpose | Refresh Lag |
|-------|---------|-------------|
| 🥉 **Bronze** | Raw data, as-is from sources | N/A (base tables) |
| 🥈 **Silver** | Cleaned, validated, enriched | 1 minute |
| 🥇 **Gold** | Business aggregations & metrics | 5 minutes |

---

## 📊 Data Flows

### Sensor Data Pipeline
```
IoT Devices → RAW_SENSOR_READINGS → SENSOR_READINGS_CLEANED → DEVICE_HEALTH_HOURLY
                                   (anomaly detection)        (hourly aggregation)
```

### Transaction Pipeline
```
POS Systems → RAW_TRANSACTIONS → TRANSACTIONS_ENRICHED → DAILY_SALES_SUMMARY
                                (validation, totals)      PRODUCT_PERFORMANCE
```

### Customer Events Pipeline
```
Web/Mobile → RAW_CUSTOMER_EVENTS → CUSTOMER_SESSIONS → CUSTOMER_BEHAVIOR_METRICS
                                  (sessionization)      (conversion analysis)
```

---

## 🛠️ CLI Reference

### Data Pipeline (`data_pipeline.py`)

```bash
# Full ingestion (all data types)
python data_pipeline.py --mode full

# Specific data types
python data_pipeline.py --mode sensors
python data_pipeline.py --mode transactions  
python data_pipeline.py --mode events

# Custom batch size
python data_pipeline.py --mode sensors --batch-size 2000

# Continuous mode (streaming simulation)
python data_pipeline.py --continuous --interval 30

# Check pipeline status
python data_pipeline.py --mode status
```

### Monitor (`monitor_pipeline.py`)

```bash
# Interactive dashboard
python monitor_pipeline.py

# Auto-refresh dashboard
python monitor_pipeline.py --watch --interval 30

# Health check
python monitor_pipeline.py --mode health

# Refresh history
python monitor_pipeline.py --mode history

# Export to JSON
python monitor_pipeline.py --mode json > status.json
```

---

## 🔍 Monitoring Queries

Run these in Snowflake to monitor your pipeline:

```sql
-- Check Dynamic Table status
SELECT * FROM GOLD.V_DYNAMIC_TABLE_STATUS;

-- Check data freshness
SELECT * FROM GOLD.V_PIPELINE_FRESHNESS;

-- View refresh history
SELECT * FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
    NAME_PREFIX => 'DATA_ENGINEERING_DEMO'
))
ORDER BY REFRESH_END_TIME DESC
LIMIT 20;

-- Check for stale data
SELECT 
    NAME,
    TIMESTAMPDIFF('minute', LAST_COMPLETED_REFRESH, CURRENT_TIMESTAMP()) AS minutes_since_refresh
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES())
WHERE DATABASE_NAME = 'DATA_ENGINEERING_DEMO'
  AND TIMESTAMPDIFF('minute', LAST_COMPLETED_REFRESH, CURRENT_TIMESTAMP()) > 10;
```

---

## 💡 Best Practices

1. **Target Lag Selection**
   - Real-time needs: 1-5 minutes
   - Batch analytics: 15-60 minutes
   - Daily reports: 24 hours

2. **Warehouse Sizing**
   - Start with XS for development
   - Scale up based on data volume and refresh frequency
   - Use dedicated warehouses for DT refresh

3. **Cost Management**
   - Monitor `DYNAMIC_TABLE_REFRESH_HISTORY`
   - Use appropriate target lag (tighter = more compute)
   - Consider time-travel retention settings

4. **Error Handling**
   - Check `SCHEDULING_STATE` for issues
   - Monitor `STATE_MESSAGE` in refresh history
   - Set up alerts for refresh failures

---

## 🧹 Cleanup

To remove all demo objects:

```sql
-- Drop database (removes all schemas, tables, dynamic tables)
DROP DATABASE IF EXISTS DATA_ENGINEERING_DEMO;

-- Drop warehouse
DROP WAREHOUSE IF EXISTS WH_DATA_ENG;
```

---

## 📚 Resources

- [Snowflake Dynamic Tables Documentation](https://docs.snowflake.com/en/user-guide/dynamic-tables-intro)
- [snowflake-connector-python](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector)
- [Medallion Architecture](https://docs.snowflake.com/en/user-guide/dynamic-tables-best-practices)

---

## 📝 License

This demo is provided for educational purposes. Use at your own discretion.

