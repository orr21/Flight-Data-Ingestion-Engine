# Flight Data Ingestion Engine

A PySpark-based ingestion engine for processing daily flight data with automated schema flattening, timezone conversion, and incremental partitioned writes.

## Overview

This package provides a complete ETL pipeline for flight data processing. It reads JSON files containing flight information, flattens nested structures, enriches data with UTC timestamps, calculates inter-flight intervals by airport, and writes results to partitioned managed tables in Spark.

## Features

- **Automated Schema Flattening**: Recursively flattens nested JSON structures and arrays
- **Timezone Conversion**: Converts local departure times to UTC using IATA airport codes
- **Incremental Processing**: Supports daily incremental loads with partition overwrite
- **Inter-Flight Analysis**: Calculates time intervals between consecutive flights from the same origin
- **Configurable Pipeline**: JSON-based configuration for schema, output tables, and partitioning
- **Unit Tested**: Comprehensive test suite for all core functionality

## Installation

### Prerequisites

- Python >= 3.10
- Java Development Kit (JDK) 8 or 11
- For Windows: Hadoop winutils and DLL library

### Install from source

```bash
# Clone the repository
git clone <repository-url>
cd spark-tarea-final

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install .
```

### Windows-specific setup

For Spark to work properly on Windows:
1. Install JDK and set `JAVA_HOME` environment variable
2. Download Hadoop winutils for Windows
3. Set `HADOOP_HOME` environment variable

## Project Structure

```
spark-tarea-final/
├── motor_ingesta/
│   ├── __init__.py
│   ├── motor_ingesta.py      # Core ingestion engine with schema flattening
│   ├── flujo_diario.py        # Daily workflow orchestration
│   ├── agregaciones.py        # UTC conversion and interval calculations
│   └── resources/
│       └── timezones.csv      # IATA to IANA timezone mapping
├── tests/
│   ├── conftest.py            # Pytest fixtures
│   ├── test_ingesta.py        # Unit tests
│   └── resources/
│       ├── test_config.json
│       └── test_data.json
├── config/
│   └── config.json            # Pipeline configuration
├── notebooks/
│   └── actividad_spark.ipynb # Usage examples
├── setup.py                   # Package setup
├── motor_ingesta-0.1.0-py3-none-any.whl # Wheel file
├── requirements.txt           # Dependencies
└── README.md                  # This file
```

## Configuration

The pipeline is configured via `config/config.json`:

```json
{
  "data_columns": [
    {
      "name": "FlightDate",
      "type": "date",
      "format": "yyyy-MM-dd",
      "comment": "Flight date"
    },
    ...
  ],
  "output_table": "default.flights",
  "output_partitions": 10,
  "EXECUTION_ENVIRONMENT": "local"
}
```

### Configuration Parameters

- **data_columns**: Array defining output schema with name, type, and metadata
- **output_table**: Fully qualified table name (database.table)
- **output_partitions**: Number of partitions for output data
- **EXECUTION_ENVIRONMENT**: `"local"` for local SparkSession

## API Reference

### FlujoDiario Class

Main orchestrator for daily flight data processing.

#### Constructor
```python
FlujoDiario(config_file: str)
```
- `config_file`: Path to JSON configuration file

#### Methods

- `procesa_diario(data_file: str)`: Orchestrates daily processing pipeline
  - Reads JSON file
  - Flattens schema
  - Adds UTC timestamps
  - Calculates inter-flight intervals
  - Writes partitioned table

### MotorIngesta Class

Core ingestion engine with schema flattening capabilities.

#### Methods

- `ingesta_fichero(json_path: str) -> DataFrame`: Ingests and flattens JSON file
- `aplana_df(df: DataFrame) -> DataFrame`: Recursively flattens nested structures (static)

### Aggregation Functions

Located in `agregaciones.py`:

- `aniade_hora_utc(spark: SparkSession, df: DataFrame) -> DataFrame`: Adds UTC timestamp column
- `aniade_intervalos_por_aeropuerto(df: DataFrame) -> DataFrame`: Adds next flight info and time intervals

## Usage Examples

### Basic Pipeline Execution

```python
from motor_ingesta.flujo_diario import FlujoDiario

# Initialize with configuration
flujo = FlujoDiario("config/config.json")

# Process daily file
flujo.procesa_diario("data/2023-01-01.json")
```

### Custom Ingestion

```python
from motor_ingesta.motor_ingesta import MotorIngesta
import json

# Load configuration
with open("config/config.json") as f:
    config = json.load(f)

# Initialize engine
motor = MotorIngesta(config)

# Ingest and flatten JSON
df = motor.ingesta_fichero("data/flights.json")
df.show()
```

### Adding UTC Timestamps

```python
from motor_ingesta.agregaciones import aniade_hora_utc
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Assuming df has FlightDate, DepTime, and Origin columns
df_with_utc = aniade_hora_utc(spark, df)
df_with_utc.select("Origin", "FlightDate", "DepTime", "FlightTime").show()
```

## Data Schema

### Input Data

JSON files with nested structures containing flight information. The engine automatically flattens arrays and structs.

### Output Schema

After processing, the output table includes:

- **Flight Information**: Date, airline, origin/destination airports
- **Timing**: Departure/arrival times, delays, air time
- **Enriched Fields**:
  - `FlightTime`: UTC timestamp of departure
  - `FlightTime_next`: UTC timestamp of next flight from same origin
  - `Airline_next`: Airline of next flight
  - `diff_next`: Seconds until next flight

## Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_ingesta.py -v

# Run with coverage
pytest tests/ --cov=motor_ingesta
```

## Building the Package

```bash
# Build wheel
python setup.py bdist_wheel

# Install locally
pip install dist/motor-ingesta-0.1.0-py3-none-any.whl
```

## License

This project is provided for educational purposes.

## Author

Óscar Rico Rodríguez (oscarico@ucm.es)

## Acknowledgments

- Built with Apache Spark and PySpark
