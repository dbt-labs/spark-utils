import uuid

import pytest
from dbt.clients.jinja import MacroGenerator
from pyspark.sql import SparkSession


@pytest.fixture
def simple_table(spark_session: SparkSession) -> str:
    """Create and delete a simple table used for testing."""
    table_name = f"default.table_{uuid.uuid4()}".replace("-", "_")
    spark_session.sql(f"CREATE TABLE {table_name} (id int) USING parquet")
    yield table_name
    spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.parametrize(
    "macro_generator", ["macro.spark_utils.get_tables"], indirect=True
)
def test_create_table(
    macro_generator: MacroGenerator, simple_table: str
) -> None:
    """The `get_tables` macro should return the created table."""
    tables = macro_generator()
    assert simple_table in tables

