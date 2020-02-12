
test-spark:
	dbt deps
	dbt compile --target spark
	dbt seed --target spark --full-refresh
	dbt run --target spark --full-refresh
	dbt test --target spark
