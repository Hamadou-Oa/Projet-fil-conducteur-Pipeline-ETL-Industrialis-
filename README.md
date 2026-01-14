# Industrial ETL Pipeline

**Apache Spark • Apache Airflow • MinIO • Docker**

##  Overview

This project implements an **industrial-grade ETL (Extract, Transform, Load) pipeline** designed to automate data ingestion from multiple heterogeneous sources, process data at scale, and store it in a modern **Data Lakehouse architecture**.

The pipeline demonstrates **real-world Data Engineering practices**, including orchestration, distributed processing, containerization, logging, configuration management, and testing.

---

##  Project Objectives

* Build a complete **end-to-end ETL pipeline**
* Automate data collection from multiple sources
* Apply **distributed transformations** using Apache Spark
* Store processed data in an **S3-compatible object storage**
* Orchestrate workflows with Apache Airflow
* Ensure reproducibility through **Docker-based deployment**
* Follow software engineering best practices

---

##  Architecture

**Data Sources**

* Web scraping (BooksToScrape)
* CSV file
* PostgreSQL database

**Processing**

* Apache Spark (distributed transformations)
* Data cleaning, normalization, and merging

**Storage**

* MinIO (S3-compatible Data Lake)
* Parquet format
* Partitioned datasets

**Orchestration**

* Apache Airflow (DAG-based workflow management)

**Deployment**

* Docker & Docker Compose

---

##  ETL Workflow

1. **Extract**

   * Web scraping using `requests` and `BeautifulSoup`
   * CSV ingestion
   * SQL extraction via PostgreSQL

2. **Transform**

   * Data validation and cleaning
   * Schema harmonization
   * Distributed processing with Spark

3. **Load**

   * Persist data into MinIO
   * Optimized storage using Parquet format
   * Partitioning by data source

4. **Orchestrate**

   * Automated execution with Airflow DAGs
   * Task dependency management
   * Logging and failure handling

---

##  Project Structure

```
pipeline-etl-industrialise/
│
├── src/
│   ├── extraction/
│   ├── transformation/
│   ├── load/
│   └── utils/
│
├── airflow/
│   └── dags/
│
├── tests/
│
├── docker/
│
├── guide/
│   └── Guide_Projet_ETL.docx
│
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## How to Run the Project

### Prerequisites

* Docker
* Docker Compose

### Start the pipeline

```bash
docker-compose up -d
```

### Access services

* **Airflow UI**: [http://localhost:8080](http://localhost:8080)
* **MinIO Console**: [http://localhost:9001](http://localhost:9001)

---

## Testing & Quality

* Unit tests implemented using `pytest`
* Configuration externalized via environment variables
* Centralized logging for traceability
* Modular and maintainable codebase

---

##  Security & Best Practices

* No credentials hardcoded
* Secrets managed via environment variables
* Clean Git history
* Reproducible environment using Docker

---

##  Skills Demonstrated

* Data Engineering fundamentals
* Distributed data processing (Spark)
* Workflow orchestration (Airflow)
* Data Lakehouse concepts
* Docker & containerization
* Software engineering best practices
* ETL pipeline design

---

##  Author

**ABADOULAHI**
 Email: [abdoulahihamadououmarou@gmail.com](mailto:abdoulahihamadououmarou@gmail.com)
 Specialiste – Data ENGENEER
 Location: Niger

---

##  Portfolio Note

This project was developed as a **capstone academic project** and reflects practical, industry-aligned data engineering workflows.
It can be extended to include additional data sources, cloud deployment, or analytical layers.

---
