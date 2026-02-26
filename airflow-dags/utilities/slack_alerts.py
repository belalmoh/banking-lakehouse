"""
Slack Alert Helpers for Airflow DAGs
Banking Lakehouse Project

Provides alert formatting and notification utilities.
In production, connect to a real Slack webhook.
"""

from datetime import datetime
from typing import Dict, Optional

from loguru import logger


# Slack webhook URL — configure in Airflow Variables or environment
SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"


def format_alert_message(
    dag_id: str,
    task_id: str,
    execution_date: str,
    status: str = "FAILED",
    details: Optional[str] = None,
) -> Dict:
    """
    Format a Slack alert message for pipeline events.

    Args:
        dag_id: Airflow DAG identifier.
        task_id: Failed task identifier.
        execution_date: DAG execution date.
        status: Alert status (FAILED, WARNING, SUCCESS).
        details: Additional context about the failure.

    Returns:
        Formatted Slack message payload.
    """
    emoji = {"FAILED": "🔴", "WARNING": "🟡", "SUCCESS": "🟢"}.get(status, "⚪")

    message = {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{emoji} Banking Lakehouse Alert — {status}",
                },
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*DAG:* `{dag_id}`"},
                    {"type": "mrkdwn", "text": f"*Task:* `{task_id}`"},
                    {"type": "mrkdwn", "text": f"*Date:* {execution_date}"},
                    {"type": "mrkdwn", "text": f"*Status:* {status}"},
                ],
            },
        ],
    }

    if details:
        message["blocks"].append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Details:*\n```{details}```"},
        })

    return message


def send_failure_alert(context: Dict) -> None:
    """
    Airflow callback function for task failure alerts.
    Use as `on_failure_callback=send_failure_alert` in DAG/task definition.
    """
    dag_id = context.get("dag", {}).dag_id if context.get("dag") else "unknown"
    task_id = context.get("task_instance", {}).task_id if context.get("task_instance") else "unknown"
    execution_date = str(context.get("execution_date", "unknown"))
    exception = str(context.get("exception", "No exception details"))

    message = format_alert_message(
        dag_id=dag_id,
        task_id=task_id,
        execution_date=execution_date,
        status="FAILED",
        details=exception[:500],  # Truncate long exceptions
    )

    logger.error(
        f"Pipeline failure alert: dag={dag_id}, task={task_id}, "
        f"date={execution_date}, error={exception[:200]}"
    )

    # In production, send to Slack:
    # import requests
    # requests.post(SLACK_WEBHOOK_URL, json=message)

    # For demo, just log the message
    logger.info(f"Slack alert payload: {message}")


def send_success_alert(context: Dict) -> None:
    """
    Airflow callback for pipeline success notifications.
    Use as `on_success_callback=send_success_alert`.
    """
    dag_id = context.get("dag", {}).dag_id if context.get("dag") else "unknown"
    execution_date = str(context.get("execution_date", "unknown"))

    logger.info(f"🟢 Pipeline SUCCESS: dag={dag_id}, date={execution_date}")
