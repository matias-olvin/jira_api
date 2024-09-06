CREATE OR REPLACE TABLE `olvin-sandbox-20210203-0azu4n.pablo_test.cvs_site_sample_zipcodes` AS
SELECT
fk_zipcodes,
primary_city,
ROUND(workers_percentage/SUM(workers_percentage) OVER ()*100, 2) AS workers_percentage,
--polygon_zipcode,
FROM
(SELECT
fk_zipcodes,
SUM(visits_table.visit_score) AS workers_percentage,
ANY_VALUE(polygon_zipcode) AS polygon_zipcode,
ANY_VALUE(primary_city) AS primary_city,
FROM `olvin-sandbox-20210203-0azu4n.pablo_test.cvs_site_sample_devices` AS visits_table
 JOIN           (SELECT home_point, device_id, local_date FROM
                `storage-prod-olvin-com.device_homes.*`
              WHERE local_date >= "2021-01-01" AND local_date < "2022-01-01") AS homes_table
              ON visits_table.device_id = homes_table.device_id AND DATE_TRUNC(visits_table.local_date, MONTH) = homes_table.local_date
JOIN (SELECT pid AS fk_zipcodes, ST_GEOGFROMTEXT(polygon) AS polygon_zipcode, primary_city FROM `storage-prod-olvin-com.area_geometries.zipcodes`)
ON ST_CONTAINS(polygon_zipcode, home_point)
GROUP BY fk_zipcodes)
JOIN(
    SELECT pid AS fk_zipcodes, polygon FROM `storage-prod-olvin-com.area_geometries.zipcodes`
)
USING(fk_zipcodes)


ORDER BY workers_percentage DESC