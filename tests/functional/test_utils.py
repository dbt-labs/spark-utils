import os
import pytest
from dbt.tests.util import run_dbt
from dbt.tests.adapter.utils.base_utils import BaseUtils
from dbt.tests.adapter.utils.test_concat import BaseConcat
from dbt.tests.adapter.utils.test_dateadd import BaseDateAdd
from dbt.tests.adapter.utils.test_datediff import BaseDateDiff
from dbt.tests.adapter.utils.test_split_part import BaseSplitPart


class BaseSparkUtilsBackCompat(BaseUtils):
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


    # actual test sequence needs to run 'deps' first
    def test_build_assert_equal(self, project):
        run_dbt(['deps'])
        super().test_build_assert_equal(project)


class TestConcat(BaseSparkUtilsBackCompat, BaseConcat):
    pass


class TestDateAdd(BaseSparkUtilsBackCompat, BaseDateAdd):
    pass


class TestDateDiff(BaseSparkUtilsBackCompat, BaseDateDiff):
    pass


class TestSplitPart(BaseSparkUtilsBackCompat, BaseSplitPart):
    pass
