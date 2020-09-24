
test-spark:
	dbt deps
	dbt seed --target spark --full-refresh
	dbt run --target spark --full-refresh
	dbt run --target spark
	dbt test --target spark

test-databricks:
	dbt deps
	dbt seed --target databricks --full-refresh
	dbt run --target databricks --full-refresh
	dbt run --target databricks
	dbt test --target databricks

