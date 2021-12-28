import dataclasses
import os

import dbt.tracking
import pytest
from _pytest.fixtures import SubRequest
from dbt.adapters.factory import get_adapter, register_adapter, AdapterContainer
from dbt.clients.jinja import MacroGenerator
from dbt.config.runtime import RuntimeConfig
from dbt.context import providers
from dbt.contracts.connection import ConnectionState
from dbt.contracts.graph.manifest import Manifest
from dbt.adapters.spark.connections import (
    SparkConnectionManager,
    PyodbcConnectionWrapper,
)
from dbt.parser.manifest import ManifestLoader
from dbt.tracking import User
from pyspark.sql import SparkSession
from sodaspark.scan import Connection


dbt.tracking.active_user = User(os.getcwd())


class _SparkConnectionManager(SparkConnectionManager):
    @classmethod
    def open(cls, connection):
        handle = PyodbcConnectionWrapper(Connection())
        connection.handle = handle
        connection.state = ConnectionState.OPEN
        return connection


@dataclasses.dataclass(frozen=True)
class Args:
    project_dir: str = os.getcwd()


@pytest.fixture
def config() -> RuntimeConfig:
    # requires a profile in your project wich also exists in your profiles file
    config = RuntimeConfig.from_args(Args())
    return config


@pytest.fixture
def adapter(config: RuntimeConfig) -> AdapterContainer:
    register_adapter(config)
    adapter = get_adapter(config)

    connection_manager = _SparkConnectionManager(adapter.config)
    adapter.connections = connection_manager

    adapter.acquire_connection()

    return adapter


@pytest.fixture
def manifest(
    adapter: AdapterContainer,
) -> Manifest:
    manifest = ManifestLoader.get_full_manifest(adapter.config)
    return manifest


@pytest.fixture
def macro_generator(
    request: SubRequest, config: RuntimeConfig, manifest: Manifest
) -> MacroGenerator:
    macro = manifest.macros[request.param]
    context = providers.generate_runtime_macro_context(
        macro, config, manifest, macro.package_name
    )
    macro_generator = MacroGenerator(macro, context)
    return macro_generator


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
