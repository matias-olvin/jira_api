INSERT INTO
    `storage-prod-olvin-com.smc_ground_truth_volume_model.categories_to_append` (
        fk_sgbrands,
        sub_category,
        median_category_visits,
        category_median_visits_range,
        median_brand_visits,
        updated_at
    )
SELECT -- an example of an insert value
    CAST(NULL AS STRING),
    'Speed Changer, Industrial High-Speed Drive, and Gear Manufacturing',
    5000,
    2.0,
    NULL,
    CAST(NULL AS DATE) -- last value should be left null (updated_at column)
UNION ALL
--add more lines here if need;