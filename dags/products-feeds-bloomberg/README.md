# Bloomberg feed to Tamoco

This pipeline sends the poi visits of all safegraph places to a GCP bucket which Tamoco pull from daily. 

## Notes
- On 30th July 2021 there is a decrease in visits since we removed the large erroneous POIs such as those in georgia.

**2022-02-23**
- Updated the code so that the feed is coming from `poi_visits.sql` and no longer dependent on the met areas table. Still uses the met area places table (gb_us..)

**2022-03-07**
- Code no longer relies on the met area places table. Solved the duplicate issue by not using simplified polygons (as they don't then have overlap) - it is much slower but works for now. 

**2022-08-18**
- Updated `poi_visits_to_places.sql` to handle converting `olvin_id` to placekey in static places table (`sg_places.20111101`).
- Schedule removed, pipeline now triggered externally from `visits_pipeline_all_daily`.
