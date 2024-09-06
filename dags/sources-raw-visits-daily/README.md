# Visits Pipeline All Daily

This pipeline uses Safegraph places data to run the visits pipeline daily for all POIS

## Notes

- **This updates to DEV&PROD at the moment.**

- Full visits pipeline from loading raw tamoco to scale poi visits.
- `poi_visits_met_areas` uses new places table (`20110101`)
- `poi_visits_met_areas_scaled` uses old places table (`gb-us-20210728`) (so can join csds and neighbourhoods).
- `device_id` encrypted by us (using old BQ encryption) from 2021-09-03 to 2021-11-14. Then takes new snowflake
  encryption from 2021-11-15.

**2022-02-15**

- Update `poi_visits.sql` code to include parent_bool and child_bool so that can then partition by parent_bool to solve
  the issue of overlap in parent polygons.
- Update `poi_visits.sql` code so that a device only gives a poi one visit when it visits a parent polygon in one day.
  If there are two or more overlapping parent polygons then we take the max visit score to these.

**2022-02-22**

- Update `device_clusters.py` code to include moving data form gcp to bq from 30 days ago. 

**2022-06-06**

- `config.yaml` is shared across POI and zipcode pipelines.
- The delay is now an environment variable, so it can be changed on all pipelines simultaneously

**2022-08-17**

- Daily pipelines merged: previously `poi_visits_pipeline_all_daily` and `zipcodes_visits_pipeline_all_daily`.
- Idempotency: `DAG` can be re-run for previous dates (backfill) without manual deletion of data in BigQuery.
- Bloomberg feed trigger: `bloomberg_feed` now triggered on completion of `query_poi_visits` Task.
- SMC Backfill: `visits_pipeline_all_daily_backfill` is used to run backfills once SMC has been completed.