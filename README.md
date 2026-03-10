# BBVA Data Pipeline


[![CI Pipeline](https://github.com/SidharthaManriquez44/bbva-digital-analysis/actions/workflows/ci.yml/badge.svg)](https://github.com/SidharthaManriquez44/bbva-digital-analysis/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/SidharthaManriquez44/bbva-data-pipeline/graph/badge.svg?token=zTvLTbVmEl)](https://codecov.io/gh/SidharthaManriquez44/bbva-data-pipeline)
![Python](https://img.shields.io/badge/python-3.10-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
![Lint: Ruff](https://img.shields.io/badge/lint-ruff-blue)
![Status](https://img.shields.io/badge/status-in%20development-orange)
![Data Platform](https://img.shields.io/badge/Data%20Platform-Engineering-blue)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen)](https://github.com/pre-commit/pre-commit)
![pyproject.toml](https://img.shields.io/badge/config-pyproject.toml-blue)
![SQLFluff](https://img.shields.io/badge/lint-SQLFluff-red)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?logo=apache-airflow&logoColor=white)
![Docker](https://img.shields.io/badge/docker-ready-blue)

## Overview

This project implements a **data pipeline for a Data Warehouse** that processes banking metrics and loads them into analytical layers.
The pipeline follows a **layered Data Warehouse architecture** and is orchestrated using **Apache Airflow**.

The system includes:

* Incremental data loads
* Data quality validation
* Multi-layer Data Warehouse loading
* Airflow orchestration using **TaskFlow API**
* Docker-based deployment

---

# Architecture

The project follows a **modular architecture inspired by Hexagonal Architecture**, separating business logic, data access, and orchestration.

```
bbva-data-pipeline
│
├── airflow/                # Airflow orchestration
│   ├── dags/
│       └── bbva_pipeline_dag.py
│
│
├── src/                    # Application layer
│
│   ├── extract/            # Data extraction logic
│   ├── transform/          # Data transformation
│   ├── load/               # Data loading to DW layers
│   ├── data_access/        # Repositories for metadata tables
│   ├── utils/              # Utilities (SQL loader, helpers)
│   └── config/             # Configuration (DB, logging)
│
├── sql/                    # SQL scripts for DW layers
│
│   ├── raw/
│   ├── staging/
│   ├── core/
│   ├── marts/
│   └── meta/
│
├── .pre-commit-config.yaml
│
├── docker-compose.yaml
│
├── data/                   # Input datasets
│
├── tests/                  # Unit tests
│
├── requirements.txt
│
└── README.md
```

---

# Data Warehouse Layers

The pipeline loads data into multiple layers:

### Raw Layer

Stores raw extracted data for traceability.

### Staging Layer

Cleaned and standardized data ready for modeling.

### Core Layer

Dimensional model tables including:

* `dim_bank`
* `dim_channel`
* `dim_date`
* `fact_bank_metrics`

### Mart Layer

Aggregated tables used for analytics and reporting.

---

# Pipeline Flow

The pipeline follows this workflow:

```
extract
   ↓
data_quality_checks
   ↓
load_raw
   ↓
load_staging
   ↓
dimensions (parallel)
   ├── dim_bank
   ├── dim_channel
   └── dim_date
   ↓
fact_table
   ↓
data_marts
```

The dimensions are executed **in parallel** to optimize pipeline performance.

---

# Data Quality Checks

The pipeline performs validation before loading data.

Checks include:

* Null value detection
* Invalid year validation
* Negative value detection
* Logical consistency checks
* Duplicate detection

Example rule:

```
digital_clients <= total_clients
```

If a validation fails, the pipeline stops.

---

# Technologies

* Python
* Apache Airflow
* Docker
* PostgreSQL
* Pandas
* SQLAlchemy

---

# Running the Pipeline

## 1. Start Airflow

```
docker compose up -d
```

Airflow UI:

```
http://localhost:8085
```

Login:

```
user: airflow
password: airflow
```

---

## 2. Enable the DAG

Activate:

```
bbva_data_pipeline
```

Then trigger a run from the UI.

---

# Example Input Dataset

```
bank_code,year,branches,atms,total_clients,digital_clients,total_loans,total_deposits,net_income
BBVA,2019,1860,13170,29000000,21000000,1500000000000,1550000000000,70000000000
```

---

# Key Features

✔ Modular architecture
✔ Incremental data loads
✔ Data quality validation
✔ Airflow orchestration
✔ Docker environment
✔ Parallel dimension loading

---

# Future Improvements

Potential improvements include:

* Data lineage tracking
* Pipeline observability
* Automated schema validation
* Integration with dbt
* Data lake storage

---

# Author

**Sidhartha Manriquez**
Data Architecture | Data Platforms | Financial Analytics Engineering
