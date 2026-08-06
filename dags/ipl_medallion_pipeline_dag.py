import os
from datetime import datetime, timedelta
import requests
import json

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
try:
    from airflow.operators.empty import EmptyOperator
except ImportError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator

# ==========================================
# STEP 1: LIVE SLACK WEBHOOK CONFIGURATION
# ==========================================
SLACK_WEBHOOK_URL = os.getenv(
    "SLACK_WEBHOOK_URL",
    "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK_URL"
)


def send_slack_alert(context, status="FAILED"):
    """Sends rich formatted Slack message cards using Block Kit."""
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    execution_date = context.get("execution_date")
    
    emoji = "🔴" if status == "FAILED" else "🟢"
    title = f"{emoji} *DATA PIPELINE ALERT: {status}*"

    slack_blocks = {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{emoji} Databricks Airflow Notification",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*DAG Name:*\n`{dag_id}`"},
                    {"type": "mrkdwn", "text": f"*Task Name:*\n`{task_instance.task_id}`"},
                    {"type": "mrkdwn", "text": f"*Pipeline Status:*\n*{status}*"},
                    {"type": "mrkdwn", "text": f"*Target Storage:*\n`AWS S3 (garvit-ipl-data-lake)`"}
                ]
            },
            {
                "type": "context",
                "elements": [
                    {"type": "mrkdwn", "text": f"📅 Execution Timestamp: {execution_date}"}
                ]
            }
        ]
    }

    try:
        requests.post(
            SLACK_WEBHOOK_URL,
            data=json.dumps(slack_blocks),
            headers={"Content-Type": "application/json"}
        )
        print(f"✅ Slack alert sent successfully for status: {status}")
    except Exception as e:
        print(f"⚠️ Failed to send Slack alert: {e}")


def on_failure_callback(context):
    send_slack_alert(context, status="FAILED")


def on_success_callback(context):
    send_slack_alert(context, status="SUCCESS")


# ==========================================
# STEP 2: DEFAULT DAG ARGUMENTS
# ==========================================
default_args = {
    "owner": "garvit_data_eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "on_failure_callback": on_failure_callback,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "start_date": datetime(2026, 1, 1),
}

# ==========================================
# STEP 3: WORKFLOW DAG DEFINITION
# ==========================================
with DAG(
    dag_id="ipl_live_streaming_lakehouse_pipeline",
    default_args=default_args,
    description="Live Streaming IPL Data Pipeline using Databricks Auto Loader, Delta Lake on S3 & Slack Alerts",
    schedule_interval="@daily",
    catchup=False,
    tags=["production", "databricks", "s3", "slack_alert", "delta_lake"],
) as dag:

    start_pipeline = EmptyOperator(task_id="start_pipeline")

    # Task 1: Ingest Raw S3 JSONs into Bronze Delta Table via Auto Loader
    ingest_bronze_task = DatabricksSubmitRunOperator(
        task_id="ingest_raw_s3_to_bronze",
        databricks_conn_id="databricks_default",
        tasks=[{
            "task_key": "ingest_bronze",
            "notebook_task": {
                "notebook_path": "/Users/garvitdhiman2002@gmail.com/01_bronze_autoloader",
            }
        }],
    )

    # Task 2: Parse JSON & Flatten into Silver Delivery Events Table
    transform_silver_task = DatabricksSubmitRunOperator(
        task_id="transform_bronze_to_silver",
        databricks_conn_id="databricks_default",
        tasks=[{
            "task_key": "transform_silver",
            "notebook_task": {
                "notebook_path": "/Users/garvitdhiman2002@gmail.com/02_silver_transformations",
            }
        }],
    )

    # Task 3: Aggregate Gold Business Datamarts & Apply Z-Ordering
    aggregate_gold_task = DatabricksSubmitRunOperator(
        task_id="aggregate_silver_to_gold",
        databricks_conn_id="databricks_default",
        tasks=[{
            "task_key": "aggregate_gold",
            "notebook_task": {
                "notebook_path": "/Users/garvitdhiman2002@gmail.com/03_gold_analytics_tables",
            }
        }],
        on_success_callback=on_success_callback,  # Triggers Slack Success Card when Gold finishes!
    )

    end_pipeline = EmptyOperator(task_id="end_pipeline")

    # Workflow Dependency Order
    start_pipeline >> ingest_bronze_task >> transform_silver_task >> aggregate_gold_task >> end_pipeline
