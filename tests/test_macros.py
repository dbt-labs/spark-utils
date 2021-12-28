import dataclasses
import os

import dbt.tracking
from dbt.adapters.factory import get_adapter, register_adapter
from dbt.clients.jinja import MacroGenerator
from dbt.config.runtime import RuntimeConfig
from dbt.context import providers
from dbt.contracts.connection import ConnectionState
from dbt.adapters.spark.connections import SparkConnectionManager, PyodbcConnectionWrapper
from dbt.parser.manifest import ManifestLoader
from dbt.tracking import User
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


args = Args()
# Sets the Spark plugin in dbt.adapters.factory.FACTORY
config = RuntimeConfig.from_args(args)

register_adapter(config)

adapter = get_adapter(config)

connection_manager = _SparkConnectionManager(adapter.config)
adapter.connections = connection_manager
adapter.acquire_connection()

manifest = ManifestLoader.get_full_manifest(config)

macro = manifest.macros["macro.spark_utils.get_tables"]

context = providers.generate_runtime_macro_context(
    macro, config, manifest, macro.package_name
)

result = MacroGenerator(macro, context)()
