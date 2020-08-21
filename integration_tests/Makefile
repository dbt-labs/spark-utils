
test-dbt-utils:
	dbt deps
	cd dbt_modules/dbt_utils/integration_tests; dbt seed --target spark --full-refresh
	cd dbt_modules/dbt_utils/integration_tests; dbt run --target spark --full-refresh
	cd dbt_modules/dbt_utils/integration_tests; dbt test --target spark

test-snowplow:
	dbt deps
	cd dbt_modules/snowplow/integration_tests; dbt seed --target databricks --full-refresh
	cd dbt_modules/snowplow/integration_tests; dbt run --target databricks --full-refresh
	cd dbt_modules/snowplow/integration_tests; dbt run --target databricks
	cd dbt_modules/snowplow/integration_tests; dbt test --target databricks
