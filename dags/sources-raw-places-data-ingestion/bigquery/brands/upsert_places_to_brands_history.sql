DECLARE date_tag STRING;
SET date_tag = CONCAT(
    CAST(CURRENT_DATE() AS STRING format('YYYY')),
    '-',
    CAST(CURRENT_DATE() AS STRING format('MM')),
    '-',
    '01'
);

MERGE INTO 
        `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.{{ params['all_brands_table'] }}` all_data
    USING (
        SELECT
            DISTINCT fk_sgbrands,
            brands,
            naics_code,
            top_category,
            sub_category
        FROM 
            `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.sg_places`
        WHERE 
            fk_sgbrands IS NOT NULL AND
            fk_sgbrands NOT LIKE '%,%'
    ) AS places
    ON 
        all_data.pid=places.fk_sgbrands AND
        all_data.naics_code=places.naics_code
    WHEN MATCHED THEN
        UPDATE SET 
            brand=places.brands,
            top_category=places.top_category,
            sub_category=places.sub_category,
            last_seen=CAST(date_tag as DATE),
            activity="active"
    WHEN NOT MATCHED THEN
        INSERT (
            pid,
            brand,
            naics_code,
            top_category,
            sub_category,
            first_seen,
            last_seen,
            activity
        )
        VALUES(
            places.fk_sgbrands,
            places.brands,
            places.naics_code,
            places.top_category,
            places.sub_category,
            CAST(date_tag as DATE),
            CAST(date_tag as DATE),
            "active"
        )