
test-databricks:
	dbt deps
	dbt seed --target databricks --full-refresh
	dbt run --target databricks --full-refresh
	dbt run --target databricks
	dbt test --target databricks

