# Site selection
This DAG is built to be triggered manually with a series of parameters that depend on the analysis to be done. 
A dataset with the chosen name is created and all tables saved there. A series of queries to extract basic data about the visits
and visitors are run, and several results are computed from the resulting tables. Finally, the final results are exported to a GCS folder and Matt is notified.

[A simplified flowchart is available in Lucidchart.](https://lucid.app/lucidchart/4b25676f-2633-4e9c-aa8d-a83c20215399/edit?invitationId=inv_83c768aa-d4ae-41ef-a145-9077b479d1c8)

[Product documentation is available in Confluence](https://olvin.atlassian.net/l/c/H23ajHHT)

## Run parameters
The parameters must form [a valid JSON](https://jsonlint.com/).

- `devices`: How to select the devices related to the site. Options: `"visitors"` or `"workers"`
- `place_ids`: String with values for `fk_sgplaces` in quotes and separated by commas. Cannot be set at the same time as
  geometry.
- `select_geometry`: Geometry of the area to select. A string that can be parsed to a valid geometry column in
  BigQuery.
- `remove_geometry`: Geometry of the area to exclude. A string that can be parsed to a valid geometry column in
  BigQuery.
- `select_geometry_no_buffer`: Geometry with no buffer so can be added in the code. A string that can be parsed to a valid geometry column in
  BigQuery.
- `start_date_string`: String with starting date for analysis in format `"YYYY-MM-DD"`.
- `end_date_string`: String with end date for analysis in format `"YYYY-MM-DD"`.
- `site_name`: A string to name the outputs of this pipeline.
- `states`: String with [two-letter identifiers](https://www.ssa.gov/international/coc-docs/states.html) for states to
  use as reference in quotes and separated by commas.  
- `region`: String with only one region. Used for local chain ranking. 

### Example Geography (GE Site)

`{
"site_name": "ge_site",
"start_date_string": "2019-01-01",
"end_date_string": "2022-01-01",
"devices": "workers",
"place_ids": "",
"select_geometry": "ST_BUFFER(ST_CONVEXHULL(ST_GEOGFROMTEXT(
\"MULTIPOINT(-85.64372888903475 38.177371651157735, -85.65162329492473 38.17756431467663, -85.65879906743179 38.17815232321597, -85.659022467982 38.17134892444842474, -85.65942349491776 38.16437267585566, -85.64862319815937 38.15875345514854, -85.6439926044531 38.15998687778036)\"
)), 20)",
"remove_geometry": "ST_BUFFER(ST_CONVEXHULL(ST_GEOGFROMTEXT(
\"MULTIPOINT(-85.65380316864183 38.165188803646195, -85.65657209589298 38.16280923084548, -85.65564298741334 38.16085633192082, -85.65292925474218 38.16054530984298, -85.65132861241094 38.16253438155413)\"
)), 5)",
"states": "\"KY\", \"IN\""
} }`

### Example PLace ID (CVS Site)

`{
"site_name": "cvs_site",
"start_date_string": "2021-01-01",
"end_date_string": "2022-01-01",
"devices": "visitors",
"place_ids": "\"222-222@627-yv4-wzf\"",
"select_geometry": "",
"remove_geometry": "",
"states": "\"NY\""
}`

### Example Geography and visitors (Lincoln Place)

`{
"site_name": "lincoln_place",
"start_date_string": "2021-01-01",
"end_date_string": "2022-01-01",
"devices": "visitors",
"place_ids": "",
"select_geometry": "ST_BUFFER(ST_CONVEXHULL(ST_GEOGFROMTEXT( \"MULTIPOINT(-89.9876924171444 38.591447875199094, -89.98780475730166 38.588004179215616, -89.98841093653816 38.58793708951073, -89.98786913032 38.5857063219599, -89.9852753964179 38.5856842631249, -89.98527219639965 38.59150426986252)\")), 10)",
"remove_geometry": "ST_BUFFER(ST_CONVEXHULL(ST_GEOGFROMTEXT( \"MULTIPOINT(-89.98803564984877 38.591715930276216, -89.98785003060104 38.58795376381923, -89.9897250862117 38.58791768651026)\")), 5)",
"states": "\"IL\", \"MO\""
}`

### Example Geography and visitors and places (Lincoln Place)
`{"site_name": "lincoln_place", 
"start_date_string": "2021-01-01", 
"end_date_string": "2022-01-01", 
"devices": "visitors", 
"place_ids": "["\'222-222@5pj-5nd-wkz\'","\'222-222@5pj-5nd-w8v\'","\'224-222@5pj-5nd-syv'","\'zzw-222@5pj-5nd-syv\'","\'223-222@5pj-5nd-syv\'","\'224-222@5pj-5nd-snq\'","\'223-222@5pj-5nd-snq\'","\'222-223@5pj-5nd-snq\'", "\'zzw-223@5pj-5nd-sqz\'"]", 
"select_geometry": "ST_BUFFER(ST_CONVEXHULL(ST_GEOGFROMTEXT( \"MULTIPOINT(-89.9876924171444 38.591447875199094, -89.98780475730166 38.588004179215616, -89.98841093653816 38.58793708951073, -89.98786913032 38.5857063219599, -89.9852753964179 38.5856842631249, -89.98527219639965 38.59150426986252)\")), 10)", 
"remove_geometry": "ST_BUFFER(ST_CONVEXHULL(ST_GEOGFROMTEXT( \"MULTIPOINT(-89.98803564984877 38.591715930276216, -89.98785003060104 38.58795376381923, -89.9897250862117 38.58791768651026)\")), 5)", 
"states": "\"IL\", \"MO\"",
"region": "\"IL\"",
"select_geometry_no_buffer": "ST_CONVEXHULL(ST_GEOGFROMTEXT( \"MULTIPOINT(-89.9876924171444 38.591447875199094, -89.98780475730166 38.588004179215616, -89.98841093653816 38.58793708951073, -89.98786913032 38.5857063219599, -89.9852753964179 38.5856842631249, -89.98527219639965 38.59150426986252)\"))" 
}`

## Place Selection
Place selection can be done in tow ways: either by providing `place_ids` or both `select_geometry` and `remove_geometry`. 
The code checks whether one and only one of the groups is made of non-empty strings and branches the pipeline accordingly.
If `remove_geometry` does not need to be used, it can be replaced by any trivial geometry such as `ST_GEOGPOINT(0, 0)`.
The branching results in two different visits processing methods but that result in a table with the same fields so the
Place Selection methodology does not need to be considered at any other point of the processing. For details regarding 
the logic, check the [corresponding section in Confluence](https://olvin.atlassian.net/wiki/spaces/OLVIN/pages/2039021571/Site+Analysis#Place-Selection).

Due to the variability of possible site geometries and sources, the expression used in queries when the centroid of the site is 
required as a reference is:
```
SELECT
  ST_CENTROID_AGG(lat_long_visit_point) AS ref_point,
FROM
  `project.ste_name.visits_table`
```

## Visitors Filtering
As described in the [corresponding section in Confluence](https://olvin.atlassian.net/wiki/spaces/OLVIN/pages/2039021571/Site+Analysis#Visitors-Filtering),
there is the option to analyze both **visitors** and **workers** to the site. In this case, the logic is implemented as an
`IF` query, so there is no task branching in the DAG. This applies to both visitors homes and visitors zipcodes computations.
As in the previous case, both options result in tables with identical schemas so there is no need to take into account this division at any later point.
However, it is important to bear in mind that `work_location` will always be `NULL` if **workers** are selected, so all the 
related result will be empty.

The worker identification logic (visited the site two months in a row) is currently hardcoded in the query. In the site for which it was tested
(GE site), resulted in the removal of most visits, so a less restrictive logic could be considered.

## Demographics
It is a very straightforward implementation of [the logic described in Confluence](https://olvin.atlassian.net/wiki/spaces/OLVIN/pages/2039021571/Site+Analysis#Demographics).
First, all data is computed as if the site were a single POI, and then it is split in tables for plotting independently.


## Trade Area
It is a very straightforward implementation of [the logic described in Confluence](https://olvin.atlassian.net/wiki/spaces/OLVIN/pages/2039021571/Site+Analysis#Trade-Area).
The geography queries are relatively slow, but that will not be an issue unless the site has a massive number of visits.



## Future Work
- Activate or deactivate places in the pipeline from the parameters
- Review and integrate Ella's and Pablo's parts
- Dry runs for queries and logs of cost both to stop execution if too expensive and to log cost of each site to inform pricing of the reports
- Split exports in directories
- Custom notifications channel
