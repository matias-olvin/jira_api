# PRODUCTS-FEEDS-BUCKETS-SAMPLE-AWS

Notes:
- Runs on first of the month
- should depend on all type 1 base table generation DAGs to be done
- needs all the data the be available in AWS and GCS (no cross cloud transfers should be done in this DAG)
- It will move data within the clouds to the correct sample buckets.
- Best to trigger dag at a part of the generation workflow where all the conditions are met
- Requires  dag_run.conf['export_date_string_no_dash'] to be pushed to it via conf
