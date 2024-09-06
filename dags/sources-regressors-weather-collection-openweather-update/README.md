# How to set up the Azure Postgres Database
1. Install `apache-airflow-backport-providers-postgres` in the composer environment.
2. Configure postgres connection in Airflow webserver.
3. Add IP to whitelist (no idea how yet).

# How to update a table in the Azure Postgres Database
1. Save data as a CSV with headers, **tabs** as separator (`\t`, it prevents conflicts with strings) and only the columns in the database (there is code to get the schema in Databricks).
2. Use a PostgresOperator to create the temporary table.
3. Use a PythonOperator to read the data from GCS into a temporary file and upload to the temporary table in Postgres.
4. Use a PostgresOperator to update the target table with the date from the temporary table and delete the temporary table.
