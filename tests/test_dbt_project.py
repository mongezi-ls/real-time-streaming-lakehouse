"""
Sanity checks on the dbt project structure and configuration.
Catches typos that would otherwise break dbt run.
"""
from pathlib import Path

import yaml

DBT_ROOT = Path(__file__).parent.parent / "transform" / "kraken_analytics"


class TestDbtProjectStructure:
    def test_dbt_project_yml_exists(self):
        assert (DBT_ROOT / "dbt_project.yml").exists()

    def test_models_directory_exists(self):
        assert (DBT_ROOT / "models").is_dir()

    def test_staging_directory_exists(self):
        assert (DBT_ROOT / "models" / "staging").is_dir()

    def test_marts_directory_exists(self):
        assert (DBT_ROOT / "models" / "marts").is_dir()


class TestDbtProjectConfig:
    def test_dbt_project_yaml_is_valid(self):
        with open(DBT_ROOT / "dbt_project.yml") as f:
            config = yaml.safe_load(f)
        assert config["name"] == "kraken_analytics"
        assert config["profile"] == "kraken_analytics"

    def test_staging_models_are_views(self):
        with open(DBT_ROOT / "dbt_project.yml") as f:
            config = yaml.safe_load(f)
        assert config["models"]["kraken_analytics"]["staging"]["+materialized"] == "view"

    def test_marts_models_are_tables(self):
        with open(DBT_ROOT / "dbt_project.yml") as f:
            config = yaml.safe_load(f)
        assert config["models"]["kraken_analytics"]["marts"]["+materialized"] == "table"


class TestExpectedModelFiles:
    EXPECTED_STAGING = {
        "stg_silver_trades.sql",
        "stg_gold_revenue_per_minute.sql",
        "stg_gold_symbol_metrics.sql",
    }
    EXPECTED_MARTS = {
        "mart_symbol_summary.sql",
        "mart_hourly_volume.sql",
    }

    def test_all_staging_models_present(self):
        actual = {p.name for p in (DBT_ROOT / "models" / "staging").glob("*.sql")}
        assert self.EXPECTED_STAGING.issubset(actual), \
            f"Missing staging models: {self.EXPECTED_STAGING - actual}"

    def test_all_mart_models_present(self):
        actual = {p.name for p in (DBT_ROOT / "models" / "marts").glob("*.sql")}
        assert self.EXPECTED_MARTS.issubset(actual), \
            f"Missing mart models: {self.EXPECTED_MARTS - actual}"
