import os
import pytest
from dbt.tests.util import run_dbt

from dbt.tests.adapter.utils.base_utils import BaseUtils
from dbt.tests.adapter.utils.test_concat import BaseConcat
from dbt.tests.adapter.utils.test_dateadd import BaseDateAdd
from dbt.tests.adapter.utils.test_datediff import BaseDateDiff
from dbt.tests.adapter.utils.test_split_part import BaseSplitPart

from dbt.tests.adapter.utils.data_types.base_data_type_macro import BaseDataTypeMacro
from dbt.tests.adapter.utils.data_types.test_type_numeric import BaseTypeNumeric


class BaseSparkUtilsBackCompat:
    # install this repo as a package
    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {"local": os.getcwd()},
                {"local": f"{os.getcwd()}/dbt-utils"}
            ]}
    
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "dispatch": [{
                "macro_namespace": "dbt_utils",
                "search_order": ["spark_utils", "dbt_utils"]
            }]
        }

    # call the macros from the 'dbt_utils' namespace
    # instead of the unspecified / global namespace
    def macro_namespace(self):
        return "dbt_utils"


class BaseSparkUtilsBackCompatUtil(BaseSparkUtilsBackCompat, BaseUtils):
    # actual test sequence needs to run 'deps' first
    def test_build_assert_equal(self, project):
        run_dbt(['deps'])
        super().test_build_assert_equal(project)


class BaseSparkUtilsBackCompatDataType(BaseSparkUtilsBackCompat, BaseDataTypeMacro):
    # actual test sequence needs to run 'deps' first
    def test_check_types_assert_match(self, project):
        run_dbt(['deps'])
        super().test_check_types_assert_match(project)


class TestConcat(BaseSparkUtilsBackCompatUtil, BaseConcat):
    pass


class TestDateAdd(BaseSparkUtilsBackCompatUtil, BaseDateAdd):
    pass


class TestDateDiff(BaseSparkUtilsBackCompatUtil, BaseDateDiff):
    pass


class TestSplitPart(BaseSparkUtilsBackCompatUtil, BaseSplitPart):
    pass


class TestTypeNumeric(BaseSparkUtilsBackCompatDataType, BaseTypeNumeric):
    def numeric_fixture_type(self):
        return "decimal(28,6)"
