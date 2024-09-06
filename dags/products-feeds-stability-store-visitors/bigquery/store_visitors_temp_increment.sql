CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['store_visitors_temp_table'] }}`
PARTITION BY month_starting
CLUSTER BY store_id
AS
with
get_visits as (
    select *
    from
        `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['postgres_batch_dataset'] }}-{{ params['sgplacedailyvisitsraw_table'] }}-{{ var.value.data_feed_data_version.replace('.', '-') }}`
        -- `storage-prod-olvin-com.postgres_final.SGPlaceDailyVisitsRaw`
),
# explode the visits
monthly_visits as (
    select
        local_date,
        SUM(CAST(visits as INT64)) total_visits,
        fk_sgplaces,
    from (
        select
            local_date,
            fk_sgplaces,
            JSON_EXTRACT_ARRAY(visits) as visit_array, -- Get an array from the JSON string
        from
            get_visits
    )
    cross join
        UNNEST(visit_array) as visits -- Convert array elements to row
        with offset as row_number -- Get the position in the array as another column
    group by
        local_date,
        fk_sgplaces

),
cameo_proportions as (
    select
        fk_sgplaces,
        CAMEO_USA,
        local_date,
        weighted_visit_score/NULLIF(SUM(weighted_visit_score) OVER(PARTITION BY fk_sgplaces, local_date), 0) as visit_p
    from
        `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['cameo_staging_dataset'] }}-{{ params['smoothed_transition_table'] }}-{{ var.value.data_feed_data_version.replace('.', '-') }}`
        -- `storage-prod-olvin-com.cameo_staging.smoothed_transition`
    where CAMEO_USA <> "XX"

),
cameo_percentages as (
    select
        fk_sgplaces,
        local_date,
        {% raw %} REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TO_JSON_STRING(visitor_psychographic_profiles), ',"percentage"', ''), '"CAMEO_USA":', ''), ':', ','), '[{', '{{'), '}]', '}}') {% endraw %} as visitor_psychographic_profiles,
    from (
        select
            fk_sgplaces,
            local_date,
            ARRAY_AGG(STRUCT(CAMEO_USA, ROUND(visit_p*100, 2) as percentage) ORDER BY CAST(REGEXP_EXTRACT(CAMEO_USA, r"([0-9]+)") as INT64), CAMEO_USA) as visitor_psychographic_profiles
        from cameo_proportions
        group by
            fk_sgplaces,
            local_date
    )
),
education_percentages as (
    select
        fk_sgplaces,
        local_date,
        {% raw %} REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TO_JSON_STRING(visitor_psychographic_profiles), ',"percentage"', ''), '"d_group":', ''), ':', ','), '[{', '{{'), '}]', '}}') {% endraw %} as visitor_educational_attainment,
    from (
        select
            fk_sgplaces,
            local_date,
            ARRAY_AGG(STRUCT(d_group, ROUND(percentage, 2) as percentage) ORDER BY d_group) as visitor_psychographic_profiles
        from (
            select
                fk_sgplaces,
                local_date,
                d_group,
                SUM(percentage) as percentage
            from (
                select
                    fk_sgplaces,
                    local_date,
                    (pEdAttain_Non + pEdAttain_Nurs_4thGrd + pEdAttain_5_6thGrd + pEdAttain_7_8thGrd + pEdAttain_9thGrd + pEdAttain_10thGrd + pEdAttain_11thGrd + pEdAttain_12thGrd)  * visit_p as NoDegree,
                    (pEdAttain_HighSch_Grad + pEdAttain_College_u1yr + pEdAttain_College_1pl_nodegree) * visit_p as HighSchGrad,
                    pEdAttain_AssocDegree * visit_p as AssocDegree,
                    pEdAttain_BachDegree * visit_p as BachDegree,
                    pEdAttain_MastDegree * visit_p as MastDegree,
                    pEdAttain_ProfSchDegree * visit_p as ProfSchDegree,
                    pEdAttain_DoctDegree * visit_p as DoctDegree
                from
                    cameo_proportions
                join
                    `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['static_demographics_data_dataset'] }}-{{ params['cameo_categories_processed_table'] }}-{{ var.value.data_feed_data_version.replace('.', '-') }}`
                    -- `storage-prod-olvin-com.static_demographics_data_v2.cameo_categories_processed`
                    USING(CAMEO_USA)
            )
            UNPIVOT(percentage FOR d_group IN (NoDegree, HighSchGrad, AssocDegree, BachDegree, MastDegree, ProfSchDegree, DoctDegree))
            group by
                fk_sgplaces,
                local_date,
                d_group
        )
        group by
            fk_sgplaces,
            local_date
    )
),
industry_percentages as (
    select
        fk_sgplaces,
        local_date,
        {% raw %} REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TO_JSON_STRING(visitor_psychographic_profiles), ',"percentage"', ''), '"d_group":', ''), ':', ','), '[{', '{{'), '}]', '}}') {% endraw %} as visitor_industry,
    from (
        select
            fk_sgplaces,
            local_date,
            ARRAY_AGG(STRUCT(d_group, ROUND(percentage, 2) as percentage) ORDER BY d_group) as visitor_psychographic_profiles
        from (
            select
                fk_sgplaces,
                local_date,
                REPLACE(d_group, "pInd_", "") as d_group,
                SUM(percentage) as percentage
            from (
                select
                    fk_sgplaces,
                    local_date,
                    pInd_AgricForFish * visit_p as pInd_AgricForFish,
                    pInd_MineExtract * visit_p as pInd_MineExtract,
                    pInd_Construct * visit_p as pInd_Construct,
                    pInd_Manuf * visit_p as pInd_Manuf,
                    pInd_WholeTrade * visit_p as pInd_WholeTrade,
                    pInd_RetTrade * visit_p as pInd_RetTrade,
                    pInd_TransWarehous * visit_p as pInd_TransWarehous,
                    pInd_Utilities * visit_p as pInd_Utilities,
                    pInd_Info * visit_p as pInd_Info,
                    pInd_FinancInsur * visit_p as pInd_FinancInsur,
                    pInd_RealEst * visit_p as pInd_RealEst,
                    pInd_ProfSciTech * visit_p as pInd_ProfSciTech,
                    pInd_Management * visit_p as pInd_Management,
                    pInd_AdminSupport * visit_p as pInd_AdminSupport,
                    pInd_Educ * visit_p as pInd_Educ,
                    pInd_HealthSoc * visit_p as pInd_HealthSoc,
                    pInd_ArtsEntsRec * visit_p as pInd_ArtsEntsRec,
                    pInd_AccomFood * visit_p as pInd_AccomFood,
                    pInd_OtherPrivat * visit_p as pInd_OtherPrivat,
                    pInd_PublicAdmin * visit_p as pInd_PublicAdmin,
                from cameo_proportions
                join
                    `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['static_demographics_data_dataset'] }}-{{ params['cameo_categories_processed_table'] }}-{{ var.value.data_feed_data_version.replace('.', '-') }}`
                    -- `storage-prod-olvin-com.static_demographics_data_v2.cameo_categories_processed`
                    USING(CAMEO_USA)
            )
            UNPIVOT(percentage FOR d_group IN (pInd_Construct, pInd_Manuf, pInd_WholeTrade, pInd_RetTrade, pInd_TransWarehous, pInd_Utilities, pInd_Info, pInd_FinancInsur, pInd_RealEst, pInd_ProfSciTech, pInd_Management, pInd_AdminSupport, pInd_Educ, pInd_HealthSoc, pInd_ArtsEntsRec, pInd_AccomFood, pInd_OtherPrivat, pInd_PublicAdmin))
            group by fk_sgplaces, local_date, d_group
        )
        group by fk_sgplaces, local_date
    )
),
age_percentages as (
    select
        fk_sgplaces,
        local_date,
        {% raw %} REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TO_JSON_STRING(visitor_psychographic_profiles), ',"percentage"', ''), '"d_group":', ''), ':', ','), '[{', '{{'), '}]', '}}') {% endraw %} as visitor_age_range,
    from (
        select
            fk_sgplaces,
            local_date,
            ARRAY_AGG(STRUCT(d_group, ROUND(percentage, 2) as percentage) ORDER BY d_group) as visitor_psychographic_profiles
        from (
            select
                fk_sgplaces,
                local_date,
                REPLACE(d_group, "pAge_", "") as d_group,
                SUM(percentage) as percentage
            from (
                select
                    fk_sgplaces,
                    local_date,
                    pAge_0_4 * visit_p as pAge_0_4,
                    pAge_5_9 * visit_p as pAge_5_9,
                    pAge_10_14 * visit_p as pAge_10_14,
                    pAge_15_17 * visit_p as pAge_15_17,
                    pAge_18_19 * visit_p as pAge_18_19,
                    pAge_20 * visit_p as pAge_20,
                    pAge_21 * visit_p as pAge_21,
                    pAge_22_24 * visit_p as pAge_22_24,
                    pAge_25_29 * visit_p as pAge_25_29,
                    pAge_30_34 * visit_p as pAge_30_34,
                    pAge_35_39 * visit_p as pAge_35_39,
                    pAge_40_44 * visit_p as pAge_40_44,
                    pAge_45_49 * visit_p as pAge_45_49,
                    pAge_50_54 * visit_p as pAge_50_54,
                    pAge_55_59 * visit_p as pAge_55_59,
                    pAge_60_61 * visit_p as pAge_60_61,
                    pAge_62_64 * visit_p as pAge_62_64,
                    pAge_65_66 * visit_p as pAge_65_66,
                    pAge_67_69 * visit_p as pAge_67_69,
                    pAge_70_74 * visit_p as pAge_70_74,
                    pAge_75_79 * visit_p as pAge_75_79,
                    pAge_80_84 * visit_p as pAge_80_84,
                    pAge_85pl * visit_p as pAge_85pl,
                from cameo_proportions
                join
                    `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['static_demographics_data_dataset'] }}-{{ params['cameo_categories_processed_table'] }}-{{ var.value.data_feed_data_version.replace('.', '-') }}`
                    -- `storage-prod-olvin-com.static_demographics_data_v2.cameo_categories_processed`
                    USING(CAMEO_USA)
            )
            UNPIVOT(percentage FOR d_group IN (pAge_0_4, pAge_5_9, pAge_10_14, pAge_15_17, pAge_18_19, pAge_20, pAge_21, pAge_22_24,
                pAge_25_29, pAge_30_34, pAge_35_39, pAge_40_44, pAge_45_49, pAge_50_54, pAge_55_59, pAge_60_61, pAge_62_64, pAge_65_66,
                pAge_67_69, pAge_70_74, pAge_75_79, pAge_80_84, pAge_85pl)
            )
            group by
                fk_sgplaces,
                local_date,
                d_group
        )
        group by
            fk_sgplaces,
            local_date
    )
),
home_percentages as (
    select
        fk_sgplaces,
        local_date,
        {% raw %} REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TO_JSON_STRING(visitor_psychographic_profiles), ',"percentage"', ''), '"d_group":', ''), ':', ','), '[{', '{{'), '}]', '}}') {% endraw %} as visitor_homeownership,
    from (
        select
            fk_sgplaces,
            local_date,
            ARRAY_AGG(STRUCT(d_group, ROUND(percentage, 2) as percentage) ORDER BY d_group) as visitor_psychographic_profiles
        from (
            select
                fk_sgplaces,
                local_date,
                REPLACE(d_group, "", "") as d_group,
                SUM(percentage) as percentage
            from (
                select
                    fk_sgplaces, local_date,
                    pTenur_Owned_wMort * visit_p as OwnedMort,
                    pTenur_Owned_Clear * visit_p as OwnedClear,
                    pTenur_RenterOcc * visit_p as RenterOcc,
                from cameo_proportions
                join
                    `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['static_demographics_data_dataset'] }}-{{ params['cameo_categories_processed_table'] }}-{{ var.value.data_feed_data_version.replace('.', '-') }}`
                    -- `storage-prod-olvin-com.static_demographics_data_v2.cameo_categories_processed`
                    USING(CAMEO_USA)
            )
            UNPIVOT(percentage FOR d_group IN (OwnedMort, OwnedClear, RenterOcc))
            group by
                fk_sgplaces,
                local_date,
                d_group
        )
        group by
            fk_sgplaces,
            local_date
    )
),
income_percentages as (
    select
        fk_sgplaces,
        local_date,
        {% raw %} REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TO_JSON_STRING(visitor_psychographic_profiles), ',"percentage"', ''), '"d_group":', ''), ':', ','), '[{', '{{'), '}]', '}}') {% endraw %} as visitor_household_income,
    from (
        select
            fk_sgplaces,
            local_date,
            ARRAY_AGG(STRUCT(d_group, ROUND(percentage, 2) as percentage) ORDER BY d_group) as visitor_psychographic_profiles
        from (
            select
                fk_sgplaces,
                local_date,
                REPLACE(d_group, "pm_USD", "") as d_group,
                SUM(percentage) as percentage
            from (
                select
                    fk_sgplaces,
                    local_date,
                    pm_uUSD10000 * visit_p as pm_USD0_USD10000,
                    pm_USD10000_USD14999 * visit_p as pm_USD10000_USD14999,
                    pm_USD15000_USD19999 * visit_p as pm_USD15000_USD19999,
                    pm_USD20000_USD24999 * visit_p as pm_USD20000_USD24999,
                    pm_USD25000_USD29999 * visit_p as pm_USD25000_USD29999,
                    pm_USD30000_USD34999 * visit_p as pm_USD30000_USD34999,
                    pm_USD35000_USD39999 * visit_p as pm_USD35000_USD39999,
                    pm_USD40000_USD44999 * visit_p as pm_USD40000_USD44999,
                    pm_USD45000_USD49999 * visit_p as pm_USD45000_USD49999,
                    pm_USD50000_USD59999 * visit_p as pm_USD50000_USD59999,
                    pm_USD60000_USD74999 * visit_p as pm_USD60000_USD74999,
                    pm_USD75000_USD99999 * visit_p as pm_USD75000_USD99999,
                    pm_USD100000_USD124999 * visit_p as pm_USD100000_USD124999,
                    pm_USD125000_USD149999 * visit_p as pm_USD125000_USD149999,
                    pm_USD150000_USD199999 * visit_p as pm_USD150000_USD199999,
                    pm_USD200000p * visit_p as over_200000,
                from cameo_proportions
                join
                    `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['static_demographics_data_dataset'] }}-{{ params['cameo_categories_processed_table'] }}-{{ var.value.data_feed_data_version.replace('.', '-') }}`
                    -- `storage-prod-olvin-com.static_demographics_data_v2.cameo_categories_processed`
                    USING(CAMEO_USA)
            )
            UNPIVOT(percentage FOR d_group IN (pm_USD0_USD10000, pm_USD10000_USD14999, pm_USD15000_USD19999, pm_USD20000_USD24999,
                pm_USD25000_USD29999, pm_USD30000_USD34999, pm_USD35000_USD39999, pm_USD40000_USD44999, pm_USD45000_USD49999,
                pm_USD50000_USD59999, pm_USD60000_USD74999, pm_USD75000_USD99999, pm_USD100000_USD124999, pm_USD125000_USD149999,
                pm_USD150000_USD199999, over_200000))
            group by
                fk_sgplaces,
                local_date,
                d_group
        )
        group by
            fk_sgplaces,
            local_date
    )
),
commute_percentages as (
    select
        fk_sgplaces,
        local_date,
        {% raw %} REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TO_JSON_STRING(visitor_psychographic_profiles), ',"percentage"', ''), '"d_group":', ''), ':', ','), '[{', '{{'), '}]', '}}') {% endraw %} as visitor_commute,
    from (
        select
            fk_sgplaces,
            local_date,
            ARRAY_AGG(STRUCT(d_group, ROUND(percentage, 2) as percentage) ORDER BY d_group) as visitor_psychographic_profiles
        from (
            select
                fk_sgplaces,
                local_date,
                REPLACE(d_group, "pTravWork_", "") as d_group, SUM(percentage) as percentage
            from (
                select
                    fk_sgplaces,
                    local_date,
                    pTravWork_DroveAlone * visit_p as pTravWork_DroveAlone,
                    pTravWork_Carpool * visit_p as pTravWork_Carpool,
                    pTravWork_Public * visit_p as pTravWork_PublicTransport,
                    pTravWork_Bike * visit_p as pTravWork_Bike,
                    pTravWork_Foot * visit_p as pTravWork_Foot,
                    pTravWork_Other * visit_p as pTravWork_Other,
                    pTravWork_AtHome * visit_p as pTravWork_WorkAtHome,
                from cameo_proportions
                join
                    `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['static_demographics_data_dataset'] }}-{{ params['cameo_categories_percentages_table'] }}-{{ var.value.data_feed_data_version.replace('.', '-') }}`
                    -- `storage-prod-olvin-com.static_demographics_data_v2.cameo_categories_percentages`
                    USING(CAMEO_USA)
            )
            UNPIVOT(percentage FOR d_group IN (pTravWork_DroveAlone, pTravWork_Carpool, pTravWork_PublicTransport, pTravWork_Bike, pTravWork_Foot, pTravWork_Other, pTravWork_WorkAtHome))
            group by
                fk_sgplaces,
                local_date,
                d_group
        )
        group by
            fk_sgplaces,
            local_date
    )
),
destinations_percentages as (
    select
        fk_sgplaces,
        local_date,
        {% raw %} REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TO_JSON_STRING(visitor_brand_destinations), ',"shared_devices"', ''), '"fk_sgbrands":', ''), ':', ','), '[{', '{{'), '}]', '}}') {% endraw %} as visitor_brand_destinations,
    from (
        select
            fk_sgplaces,
            local_date,
            ARRAY_AGG(STRUCT(brands.name as fk_sgbrands, ROUND(shared_devices/total_devices*100, 2) as shared_devices) ORDER BY shared_devices DESC) as visitor_brand_destinations
        from
          `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['destinations_dataset'] }}-{{ params['destinations_history_table'] }}-{{ var.value.data_feed_data_version.replace('.', '-') }}`
        --   `storage-prod-olvin-com.visitor_brand_destinations.observed_connections_history`
        inner join
          `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['postgres_batch_dataset'] }}-{{ params['sgbrandraw_table'] }}-{{ var.value.data_feed_places_version.replace('.', '-') }}` as brands
          -- `storage-prod-olvin-com.postgres_final.SGBrandRaw` as brands
        on brands.pid = fk_sgbrands
        where shared_devices > 0
        group by
            fk_sgplaces,
            local_date
    )
),
visitor_home as(
    select
        fk_sgplaces,
        local_date,
        {% raw %} REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TO_JSON_STRING(ARRAY_AGG(STRUCT(fk_zipcodes, visit_score))), ',"visit_score"', ''), '"fk_zipcodes":', ''), ':', ','), '[{', '{{'), '}]', '}}') {% endraw %} as visitor_home_locations
    from
        `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['home_dataset'] }}-{{ params['zipcodes_table'] }}-{{ var.value.data_feed_data_version.replace('.', '-') }}`
        -- `storage-prod-olvin-com.visitors_home.zipcode`
    group by
        fk_sgplaces,
        local_date
)
select
    places.pid as store_id,
    places.name,
    brands.name as brand,
    street_address,
    city,
    region as state,
    postal_code as zip_code,
    local_date as month_starting,
    DATE_SUB(DATE_ADD(local_date, INTERVAL 1 MONTH), INTERVAL 1 DAY) as month_ending,
    total_visits,
    visitor_brand_destinations,
    visitor_psychographic_profiles,
    visitor_educational_attainment,
    visitor_industry,
    visitor_age_range,
    visitor_homeownership,
    visitor_household_income,
    visitor_commute
from  -- Ensure we provide exact same stores in visitors and visits feeds: postgres
    `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['postgres_batch_dataset'] }}-{{ params['sgplaceraw_table'] }}-{{ var.value.data_feed_places_version.replace('.', '-') }}` as places
    -- `storage-prod-olvin-com.postgres_final.SGPlaceRaw` as places
join -- Idem as above
    `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['postgres_batch_dataset'] }}-{{ params['sgbrandraw_table'] }}-{{ var.value.data_feed_places_version.replace('.', '-') }}` as brands
    -- `storage-prod-olvin-com.postgres_final.SGBrandRaw` as brands
    ON brands.pid = fk_sgbrands
join(
    SELECT fk_sgplaces as activity_fk_sgplaces
    FROM
    `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['postgres_batch_dataset'] }}-{{ params['sgplaceactivity_table'] }}-{{ var.value.data_feed_data_version.replace('.', '-') }}` as brands
    -- `storage-prod-olvin-com.postgres_final.SGPlaceActivity`
    WHERE activity in('active', 'limited_data')
)
    ON places.pid = activity_fk_sgplaces
join monthly_visits
    ON places.pid = fk_sgplaces
join cameo_percentages
    USING (local_date, fk_sgplaces)
join education_percentages
    USING(local_date, fk_sgplaces)
join industry_percentages
    USING(local_date, fk_sgplaces)
join age_percentages
    USING(local_date, fk_sgplaces)
join home_percentages
    USING(local_date, fk_sgplaces)
join income_percentages
    USING(local_date, fk_sgplaces)
join commute_percentages
    USING(local_date, fk_sgplaces)
join destinations_percentages
    USING(local_date, fk_sgplaces)

where
    monthly_visits.local_date >= DATE_TRUNC('{{ ds }}', MONTH)
    AND monthly_visits.local_date < DATE_TRUNC(DATE_ADD('{{ ds }}', INTERVAL 1 MONTH), MONTH)
