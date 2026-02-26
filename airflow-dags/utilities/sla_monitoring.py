"""
SLA Monitoring Utilities for Airflow DAGs
Banking Lakehouse Project

Tracks SLA compliance and generates reports on pipeline performance.
"""

from datetime import datetime, timedelta
from typing import Dict, List

from loguru import logger


# SLA definitions per DAG (in minutes)
SLA_DEFINITIONS = {
    "banking_lakehouse_pipeline": 120,  # 2 hours
    "data_quality_monitoring": 60,       # 1 hour
    "regulatory_reporting": 240,         # 4 hours
}


def check_sla_compliance(
    dag_id: str,
    start_time: datetime,
    end_time: datetime,
) -> Dict:
    """
    Check if a DAG run completed within its SLA.

    Args:
        dag_id: DAG identifier.
        start_time: Run start time.
        end_time: Run end time.

    Returns:
        SLA compliance report.
    """
    duration_minutes = (end_time - start_time).total_seconds() / 60
    sla_minutes = SLA_DEFINITIONS.get(dag_id, 120)

    is_compliant = duration_minutes <= sla_minutes
    margin_minutes = sla_minutes - duration_minutes

    report = {
        "dag_id": dag_id,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "duration_minutes": round(duration_minutes, 2),
        "sla_minutes": sla_minutes,
        "is_compliant": is_compliant,
        "margin_minutes": round(margin_minutes, 2),
    }

    if is_compliant:
        logger.info(
            f"✅ SLA MET: {dag_id} completed in {duration_minutes:.1f}min "
            f"(SLA: {sla_minutes}min, margin: {margin_minutes:.1f}min)"
        )
    else:
        logger.warning(
            f"❌ SLA BREACH: {dag_id} took {duration_minutes:.1f}min "
            f"(SLA: {sla_minutes}min, exceeded by {abs(margin_minutes):.1f}min)"
        )

    return report


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """
    Airflow callback for SLA misses.
    Configure in DAG: `sla_miss_callback=sla_miss_callback`
    """
    dag_id = dag.dag_id

    logger.error(f"🚨 SLA MISS DETECTED: {dag_id}")
    logger.error(f"  Tasks: {[str(t) for t in task_list]}")
    logger.error(f"  Blocking: {[str(t) for t in blocking_task_list]}")
    logger.error(f"  SLAs: {slas}")

    # In production, send alert to Slack/PagerDuty
