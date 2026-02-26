"""
Integration Tests: dbt Models
Banking Lakehouse Project

Tests dbt model compilation and basic SQL correctness.
Note: Full dbt integration requires a running Spark Thrift server.
"""

import os
import subprocess
import pytest


DBT_PROJECT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "dbt-project",
)


class TestDbtModels:
    """Test suite for dbt model validation."""

    @pytest.mark.skipif(
        not os.path.exists(os.path.join(DBT_PROJECT_DIR, "dbt_project.yml")),
        reason="dbt project not found",
    )
    def test_dbt_project_yml_exists(self):
        """Verify dbt_project.yml exists and is valid."""
        project_file = os.path.join(DBT_PROJECT_DIR, "dbt_project.yml")
        assert os.path.exists(project_file)

        with open(project_file) as f:
            content = f.read()
            assert "banking_lakehouse" in content
            assert "silver" in content
            assert "gold" in content

    def test_silver_models_exist(self):
        """Verify all Silver model SQL files exist."""
        silver_dir = os.path.join(DBT_PROJECT_DIR, "models", "silver")

        expected_models = [
            "transactions_cleansed.sql",
            "customers_cleansed.sql",
            "accounts_cleansed.sql",
            "aml_alerts_cleansed.sql",
            "schema.yml",
        ]

        for model in expected_models:
            model_path = os.path.join(silver_dir, model)
            assert os.path.exists(model_path), f"Missing Silver model: {model}"

    def test_gold_models_exist(self):
        """Verify all Gold model SQL files exist."""
        gold_dir = os.path.join(DBT_PROJECT_DIR, "models", "gold")

        expected_models = [
            "customer_360.sql",
            "aml_summary.sql",
            "regulatory_report.sql",
            "schema.yml",
        ]

        for model in expected_models:
            model_path = os.path.join(gold_dir, model)
            assert os.path.exists(model_path), f"Missing Gold model: {model}"

    def test_macros_exist(self):
        """Verify all macro files exist."""
        macros_dir = os.path.join(DBT_PROJECT_DIR, "macros")

        expected_macros = [
            "convert_currency.sql",
            "calculate_risk_score.sql",
            "data_quality_checks.sql",
        ]

        for macro in expected_macros:
            macro_path = os.path.join(macros_dir, macro)
            assert os.path.exists(macro_path), f"Missing macro: {macro}"

    def test_silver_models_have_incremental_config(self):
        """Verify Silver models use incremental materialization."""
        silver_dir = os.path.join(DBT_PROJECT_DIR, "models", "silver")

        for sql_file in os.listdir(silver_dir):
            if sql_file.endswith(".sql"):
                filepath = os.path.join(silver_dir, sql_file)
                with open(filepath) as f:
                    content = f.read()
                    assert "incremental" in content, (
                        f"{sql_file} should use incremental materialization"
                    )

    def test_gold_models_reference_silver(self):
        """Verify Gold models reference Silver models (not Bronze directly)."""
        gold_dir = os.path.join(DBT_PROJECT_DIR, "models", "gold")

        for sql_file in os.listdir(gold_dir):
            if sql_file.endswith(".sql"):
                filepath = os.path.join(gold_dir, sql_file)
                with open(filepath) as f:
                    content = f.read()
                    assert "ref(" in content, (
                        f"{sql_file} should reference other models via ref()"
                    )
