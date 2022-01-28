from __future__ import annotations

import pytest
from dbt.clients.jinja import MacroGenerator
from pyspark.sql import SparkSession


@pytest.mark.parametrize(
    "macro_generator", ["macro.spark_utils.get_tables"], indirect=True
)
def test_create_table(
    spark_session: SparkSession, macro_generator: MacroGenerator
) -> None:
    expected_table = "default.example"
    spark_session.sql(f"CREATE TABLE {expected_table} (id int) USING parquet")
    tables = macro_generator()
    assert tables == [expected_table]
