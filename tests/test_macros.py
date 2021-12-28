import dataclasses
import os

import dbt.tracking
from dbt.adapters.factory import get_adapter, register_adapter
from dbt.clients.jinja import MacroGenerator
from dbt.config.runtime import RuntimeConfig
from dbt.config.project import Project
from dbt.context import providers
from dbt.adapters.spark import SparkCredentials
from dbt.parser.manifest import ManifestLoader
from dbt.tracking import User


dbt.tracking.active_user = User(os.getcwd())


@dataclasses.dataclass(frozen=True)
class Args:
    project_dir: str = os.getcwd()


credentials = SparkCredentials(
    database="default",
    schema="default",
    host="localhost",
    method="pyspark"
)

args = Args()
# Sets the Spark plugin in dbt.adapters.factory.FACTORY
config = RuntimeConfig.from_args(args)

config = Project.partial_load(os.getcwd())
config.credentials = credentials

register_adapter(config)

adapter = get_adapter(config)
adapter.acquire_connection()

manifest = ManifestLoader.get_full_manifest(config)

macro = manifest.macros["macro.spark_utils.get_tables"]

context = providers.generate_runtime_macro_context(
    macro, config, manifest, macro.package_name
)

result = MacroGenerator(macro, context)()

