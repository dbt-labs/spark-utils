import dataclasses
import os

import dbt.tracking
from dbt.adapters.factory import get_adapter, register_adapter
from dbt.clients.jinja import MacroGenerator
from dbt.config.runtime import RuntimeConfig
from dbt.context import providers
from dbt.parser.manifest import ManifestLoader
from dbt.tracking import User


dbt.tracking.active_user = User(os.getcwd())


@dataclasses.dataclass(frozen=True)
class Args:
    project_dir: str = os.getcwd()


args = Args()
config = RuntimeConfig.from_args(args)

register_adapter(config)

adapter = get_adapter(config)
adapter.acquire_connection()

manifest = ManifestLoader.get_full_manifest(config)

macro = manifest.macros["macro.spark_utils.get_tables"]

context = providers.generate_runtime_macro_context(
    macro, config, manifest, macro.package_name
)

result = MacroGenerator(macro, context)()

