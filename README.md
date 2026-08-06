# 🏏 IPL Real-Time Medallion Data Lakehouse Pipeline

[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=Databricks&logoColor=white)](https://databricks.com/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-000000?style=for-the-badge&logo=delta&logoColor=white)](https://delta.io/)
[![AWS S3](https://img.shields.io/badge/AWS%20S3-569A31?style=for-the-badge&logo=amazons3&logoColor=white)](https://aws.amazon.com/s3/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)

An end-to-end production-ready **Medallion Data Lakehouse Pipeline** designed to ingest, cleanse, transform, and analyze multi-season **Indian Premier League (IPL)** ball-by-ball match data. 

Built using **Databricks Auto Loader**, **PySpark**, **Delta Lake on AWS S3**, **Databricks SQL**, and orchestrated by **Apache Airflow** in **Docker** with automated **Slack Block Kit** notifications.

---

## 📌 Architecture Overview

```text
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                   DATA SOURCE LAYER                                    │
│             AWS S3 Storage (s3://garvit-ipl-data-lake/raw/ipl_json/*.json)             │
└───────────────────────────────────────────┬────────────────────────────────────────────┘
                                            │
                                            ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                    BRONZE LAYER                                        │
│          ⚡ Databricks Auto Loader (cloudFiles) Streaming Raw JSON Ingestion          │
│          • Schema Inference & Schema Evolution (addNewColumns)                          │
│          • Delta Table: workspace.default.matches_bronze                               │
└───────────────────────────────────────────┬────────────────────────────────────────────┘
                                            │
                                            ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                    SILVER LAYER                                        │
│         🔄 PySpark JSON Array Exploding, Deduplication & Type Enforcement              │
│          • Composite Primary Key: MatchID_BattingTeam_Over_BallNumber                  │
│          • Delta Table: workspace.default.deliveries_silver                            │
└───────────────────────────────────────────┬────────────────────────────────────────────┘
                                            │
                                            ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                     GOLD LAYER                                         │
│               📊 Aggregated Datamarts & Z-Ordering Optimization                        │
│ ┌──────────────────┬─────────────────┬──────────────────┬──────────────────────────┐   │
│ │ Team Performance │  Batter Stats   │   Bowler Stats   │      Venue Summary       │   │
│ └──────────────────┴─────────────────┴──────────────────┴──────────────────────────┘   │
└───────────────────────────────────────────┬────────────────────────────────────────────┘
                                            │
                                            ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                               ORCHESTRATION & MONITORING                               │
│  🚀 Apache Airflow (Dockerized LocalExecutor) ──► 🔔 Slack Notifications (Block Kit)   │
│  💰 EC2 Spot Instances (i3.xlarge) saving ~70% cloud compute costs                     │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🌟 Technical Highlights & Engineering Design

* **Streaming Data Ingestion**: Uses Databricks Auto Loader (`cloudFiles`) to incrementally process raw JSON telemetry payloads landing in AWS S3 without expensive file listing calls.
* **Schema Evolution & Rescue**: Handles schema changes dynamically using `cloudFiles.schemaEvolutionMode = "addNewColumns"` to prevent stream breakages during IPL match structure updates.
* **Complex Nested JSON Flattening**: Uses PySpark `explode()` and nested struct extraction to transform multi-level JSON arrays (`innings -> overs -> deliveries`) into relational event records.
* **Data Quality & Deduplication**: Generates deterministic composite primary keys (`MatchID_BattingTeam_Over_Ball`) and applies windowed deduplication to enforce data integrity.
* **Z-Ordering Performance Optimization**: Executes `OPTIMIZE ... ZORDER BY (season, batter)` on Gold Delta tables, enabling multi-dimensional data skipping to accelerate Databricks SQL queries by up to 10x.
* **Enterprise Security & Zero Secrets**: Authenticates to AWS S3 using **AWS IAM Instance Profiles** (`arn:aws:iam::...:instance-profile/databricks-s3-access-role`), eliminating static access keys from codebase.
* **Cost Optimization**: Spark execution clusters run on **AWS EC2 Spot Instances** (`i3.xlarge`), reducing compute costs by **~70%**.
* **Real-time Alerting**: Airflow callbacks format rich Slack **Block Kit** cards detailing DAG name, task status, execution timestamp, and target S3 paths.

---

## ⚙️ Detailed Pipeline Stages & Execution Flow

### 🥉 1. Bronze Layer (`01_bronze_autoloader.ipynb`)
- Ingests raw match JSON payloads incrementally using Auto Loader.
- Stores raw payloads with metadata (`_input_file_name`, `_ingestion_timestamp`) into Delta Lake.
- **S3 Path**: `s3://garvit-ipl-data-lake/bronze/matches`
- **Table**: `workspace.default.matches_bronze`

### 🥈 2. Silver Layer (`02_silver_transformations.ipynb`)
- Parses nested JSON structures into granular ball-by-ball delivery events.
- Performs schema casting, replaces null values, and calculates extra runs, wicket types, and bowler boundaries.
- Generates composite keys to eliminate duplicate delivery telemetry records.
- **S3 Path**: `s3://garvit-ipl-data-lake/silver/deliveries`
- **Table**: `workspace.default.deliveries_silver`

### 🥇 3. Gold Layer (`03_gold_analytics_tables.ipynb`)
- Aggregates business datamarts across four key domain dimensions:
  1. `gold_team_performance`: Total runs, wickets lost, boundaries, and run rates per team per season.
  2. `gold_batter_stats`: Runs scored, balls faced, strike rates, batting averages, 4s, 6s.
  3. `gold_bowler_stats`: Overs bowled, runs conceded, wickets taken, economy rates.
  4. `gold_venue_summary`: Matches played, average runs per match/ball per venue.
- Runs Delta Lake `OPTIMIZE` with `ZORDER BY` on high-cardinality predicate columns.
- **S3 Base Path**: `s3://garvit-ipl-data-lake/gold/`

---

## 🚀 Airflow Orchestration & DAG Architecture

The pipeline is orchestrated by Apache Airflow using the `DatabricksSubmitRunOperator`.

```python
start_pipeline >> ingest_bronze_task >> transform_silver_task >> aggregate_gold_task >> end_pipeline
```

### DAG Workflow Logic:
1. `start_pipeline`: Entry dummy marker.
2. `ingest_raw_s3_to_bronze`: Triggers Databricks Auto Loader notebook job.
3. `transform_bronze_to_silver`: Triggers PySpark transformation job upon Bronze completion.
4. `aggregate_silver_to_gold`: Computes Gold Datamarts, applies Z-Ordering, and invokes `on_success_callback` to trigger Slack notifications.
5. `end_pipeline`: Completion marker.

---

## 📸 Pipeline Execution & Monitoring Screenshots

| Apache Airflow DAG Execution | Slack Alert Notification |
| :---: | :---: |
| ![Airflow DAG Success](docs/images/airflow_dag_success.png) | ![Slack Block Kit Alert](docs/images/slack_notification.png) |
| *Live Airflow DAG running all tasks successfully* | *Slack Block Kit notification card on pipeline completion* |


---

## 📊 Sample Databricks SQL Queries

Once Gold datamarts are refreshed, business analysts can execute analytical SQL queries in Databricks SQL:

### 1. Top 10 Run-Scorers in IPL History (Strike Rate & Boundaries)
```sql
SELECT 
    batter,
    total_runs,
    balls_faced,
    ROUND((total_runs * 100.0 / NULLIF(balls_faced, 0)), 2) AS strike_rate,
    fours,
    sixes
FROM workspace.default.gold_batter_stats
ORDER BY total_runs DESC
LIMIT 10;
```

### 2. Most Economical Bowlers (Min 20 Overs Bowled)
```sql
SELECT 
    bowler,
    overs_bowled,
    runs_conceded,
    wickets,
    economy_rate
FROM workspace.default.gold_bowler_stats
WHERE overs_bowled >= 20
ORDER BY economy_rate ASC
LIMIT 10;
```

---

## 📂 Repository Directory Layout

```directory
ipl-data-lakehouse-pipeline/
├── dags/
│   └── ipl_medallion_pipeline_dag.py     # Airflow DAG workflow definition & Slack alerts
├── notebooks/
│   ├── 01_bronze_autoloader.ipynb        # Databricks Auto Loader streaming ingestion to Bronze
│   ├── 02_silver_transformations.ipynb   # Ball-by-ball JSON flattening & Silver cleaning
│   ├── 03_gold_analytics_tables.ipynb    # Aggregated business datamarts & Z-Ordering
│   └── Analytics_SQL.ipynb               # Databricks SQL reporting & analytics queries
├── docker-compose.yml                    # Multi-container Airflow LocalExecutor setup
├── .env.example                          # Environment variables template
├── requirements.txt                      # Python dependencies
├── .gitignore                            # Git exclusion rules
└── README.md                             # Comprehensive project documentation
```

---

## 🛠️ Local Environment & Deployment Setup

### 1. Prerequisites
- **AWS Account** with S3 Bucket (`garvit-ipl-data-lake`) and Databricks IAM Instance Profile.
- **Databricks Workspace** running Runtime 13.3+ LTS.
- **Docker Desktop** installed locally.

### 2. Configure Environment Variables
Copy `.env.example` to create your local `.env`:
```bash
cp .env.example .env
```

### 3. Spin Up Airflow via Docker Compose
```bash
docker-compose up -d
```
Access the Airflow Web UI at `http://localhost:8080` (Username: `airflow` / Password: `airflow`).

### 4. Configure Databricks Airflow Connection
In Airflow UI (`Admin -> Connections`), add `databricks_default`:
- **Conn Id**: `databricks_default`
- **Conn Type**: `Databricks`
- **Host**: `https://<your-databricks-instance>.cloud.databricks.com`
- **PAT Token**: `<your-databricks-personal-access-token>`

---

## 📜 License
This project is open-source and licensed under the [MIT License](LICENSE).
