CREATE OR REPLACE TABLE
`{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['state_demographics_table'] }}`
CLUSTER BY source, stats_group AS
WITH
geospatial_census_join AS (
  SELECT
    source,
    percentage,
    statistic,
  FROM
    (
      SELECT
        *
      FROM
        (
          SELECT
            "US Census" AS source,
            SUM(population_count) AS population_count,
            SUM(white) / SUM(population_count) * 100 AS white,
            SUM(black_or_african_american) / SUM(population_count) * 100 AS black_or_african_american,
            SUM(american_indian_or_alaskan_native) / SUM(population_count) * 100 AS american_indian_or_alaskan_native,
            SUM(asian) / SUM(population_count) * 100 AS asian,
            SUM(native_hawaiian_and_other_pacific_islander) / SUM(population_count) * 100 AS native_hawaiian_and_other_pacific_islander,
            SUM(other_race) / SUM(population_count) * 100 AS other_race,
            SUM(two_or_more_races) / SUM(population_count) * 100 AS two_or_more_races,
            SUM(pop_under_10) / SUM(population_count) * 100 AS pop_0_10,
            SUM(pop_10_to_19) / SUM(population_count) * 100 AS pop_10_to_19,
            SUM(pop_20_to_29) / SUM(population_count) * 100 AS pop_20_to_29,
            SUM(pop_30_to_39) / SUM(population_count) * 100 AS pop_30_to_39,
            SUM(pop_40_to_49) / SUM(population_count) * 100 AS pop_40_to_49,
            SUM(pop_50_to_59) / SUM(population_count) * 100 AS pop_50_to_59,
            SUM(pop_60_to_69) / SUM(population_count) * 100 AS pop_60_to_69,
            SUM(pop_70_to_79) / SUM(population_count) * 100 AS pop_70_to_79,
            SUM(pop_80_plus) / SUM(population_count) * 100 AS pop_80_plus,
            SUM(Less_than__10000____ * Total_Household_Estimate) / SUM(Total_Household_Estimate) AS income_000k_010k,
            SUM(
              _10000___14999_Estimate____ * Total_Household_Estimate
            ) / SUM(Total_Household_Estimate) AS income_010k_015k,
            SUM(
              _15000____24999_Estimate____ * Total_Household_Estimate
            ) / SUM(Total_Household_Estimate) AS income_015k_025k,
            SUM(
              _25000____34999_Estimate____ * Total_Household_Estimate
            ) / SUM(Total_Household_Estimate) AS income_025k_035k,
            SUM(
              _35000____49999_Estimate____ * Total_Household_Estimate
            ) / SUM(Total_Household_Estimate) AS income_035k_050k,
            SUM(
              _50000____74999_Estimate____ * Total_Household_Estimate
            ) / SUM(Total_Household_Estimate) AS income_050k_075k,
            SUM(
              _75000___99999_Estimate___ * Total_Household_Estimate
            ) / SUM(Total_Household_Estimate) AS income_075k_100k,
            SUM(
              _100000__149999_Estimate____ * Total_Household_Estimate
            ) / SUM(Total_Household_Estimate) AS income_100k_150k,
            SUM(
              _150000__199999_Estimate___ * Total_Household_Estimate
            ) / SUM(Total_Household_Estimate) AS income_150k_200k,
            SUM(
              _200000_or_more_Estimate_____ * Total_Household_Estimate
            ) / SUM(Total_Household_Estimate) AS income_200k_plus,
            SUM(
              _18_24_Less_than_High_School_Graduate + _25_and_Over___Less_than_9th_Grade + _25_and_Over___9th_to_12th_Grade
            ) / SUM(
              Total_Population_25_and_over + Total_Population_18_24_years
            ) * 100 AS education_0_no_high_school,
            SUM(
              _18_24_High_School_Graduate + _25_and_Over__High_School_Graduate + _25_and_Over___College__no_degree
            ) / SUM(
              Total_Population_25_and_over + Total_Population_18_24_years
            ) * 100 AS education_1_high_school_grad,
            SUM(
              _18_24_College_or_Associate_Degree + _25_and_Over___Associates_degree
            ) / SUM(
              Total_Population_25_and_over + Total_Population_18_24_years
            ) * 100 AS education_2_asociate_degree,
            SUM(
              _18_24_Bachelor_s_Degree_or_higher + _25_and_Over___Bachelor_s_Degree
            ) / SUM(
              Total_Population_25_and_over + Total_Population_18_24_years
            ) * 100 AS education_3_bachelor_degree,
            SUM(_25_and_Over___Graduate_or_Professional_Degree) / SUM(
              Total_Population_25_and_over + Total_Population_18_24_years
            ) * 100 AS education_4_grad_or_prof_degree,
          FROM
             (
              SELECT
                *,
                FORMAT("%05d", zip) AS fk_zipcodes,
              FROM
                `storage-dev-olvin-com.static_demographics_data.census_zipcodes`
            WHERE state IN ({{ dag_run.conf['states'] }})
            )
            JOIN (
              SELECT
                SUBSTR(NAME, -5, 5) AS fk_zipcodes,
                IF(
                  Less_than__10000____ < 0,
                  NULL,
                  Less_than__10000____
                ) AS Less_than__10000____,
                IF(
                  _10000___14999_Estimate____ < 0,
                  NULL,
                  _10000___14999_Estimate____
                ) AS _10000___14999_Estimate____,
                IF(
                  _15000____24999_Estimate____ < 0,
                  NULL,
                  _15000____24999_Estimate____
                ) AS _15000____24999_Estimate____,
                IF(
                  _25000____34999_Estimate____ < 0,
                  NULL,
                  _25000____34999_Estimate____
                ) AS _25000____34999_Estimate____,
                IF(
                  _35000____49999_Estimate____ < 0,
                  NULL,
                  _35000____49999_Estimate____
                ) AS _35000____49999_Estimate____,
                IF(
                  _50000____74999_Estimate____ < 0,
                  NULL,
                  _50000____74999_Estimate____
                ) AS _50000____74999_Estimate____,
                IF(
                  _75000___99999_Estimate___ < 0,
                  NULL,
                  _75000___99999_Estimate___
                ) AS _75000___99999_Estimate___,
                IF(
                  _100000__149999_Estimate____ < 0,
                  NULL,
                  _100000__149999_Estimate____
                ) AS _100000__149999_Estimate____,
                IF(
                  _150000__199999_Estimate___ < 0,
                  NULL,
                  _150000__199999_Estimate___
                ) AS _150000__199999_Estimate___,
                IF(
                  _200000_or_more_Estimate_____ < 0,
                  NULL,
                  _200000_or_more_Estimate_____
                ) AS _200000_or_more_Estimate_____,
                Total_Household_Estimate,
              FROM
                `storage-dev-olvin-com.static_demographics_data.zipcode_income`
            ) USING(fk_zipcodes)
            JOIN (
              SELECT
                SUBSTR(Zipcode, -5, 5) AS fk_zipcodes,
                IF(
                  Total_Population_18_24_years < 0,
                  NULL,
                  Total_Population_18_24_years
                ) AS Total_Population_18_24_years,
                IF(
                  _18_24_Less_than_High_School_Graduate < 0,
                  NULL,
                  _18_24_Less_than_High_School_Graduate
                ) AS _18_24_Less_than_High_School_Graduate,
                IF(
                  _18_24_High_School_Graduate < 0,
                  NULL,
                  _18_24_High_School_Graduate
                ) AS _18_24_High_School_Graduate,
                IF(
                  _18_24_College_or_Associate_Degree < 0,
                  NULL,
                  _18_24_College_or_Associate_Degree
                ) AS _18_24_College_or_Associate_Degree,
                IF(
                  _18_24_Bachelor_s_Degree_or_higher < 0,
                  NULL,
                  _18_24_Bachelor_s_Degree_or_higher
                ) AS _18_24_Bachelor_s_Degree_or_higher,
                IF(
                  Total_Population_25_and_over < 0,
                  NULL,
                  Total_Population_25_and_over
                ) AS Total_Population_25_and_over,
                IF(
                  _25_and_Over___Less_than_9th_Grade < 0,
                  NULL,
                  _25_and_Over___Less_than_9th_Grade
                ) AS _25_and_Over___Less_than_9th_Grade,
                IF(
                  _25_and_Over___9th_to_12th_Grade < 0,
                  NULL,
                  _25_and_Over___9th_to_12th_Grade
                ) AS _25_and_Over___9th_to_12th_Grade,
                IF(
                  _25_and_Over__High_School_Graduate < 0,
                  NULL,
                  _25_and_Over__High_School_Graduate
                ) AS _25_and_Over__High_School_Graduate,
                IF(
                  _25_and_Over___College__no_degree < 0,
                  NULL,
                  _25_and_Over___College__no_degree
                ) AS _25_and_Over___College__no_degree,
                IF(
                  _25_and_Over___Associates_degree < 0,
                  NULL,
                  _25_and_Over___Associates_degree
                ) AS _25_and_Over___Associates_degree,
                IF(
                  _25_and_Over___Bachelor_s_Degree < 0,
                  NULL,
                  _25_and_Over___Bachelor_s_Degree
                ) AS _25_and_Over___Bachelor_s_Degree,
                IF(
                  _25_and_Over___Graduate_or_Professional_Degree < 0,
                  NULL,
                  _25_and_Over___Graduate_or_Professional_Degree
                ) AS _25_and_Over___Graduate_or_Professional_Degree,
              FROM
                `storage-dev-olvin-com.static_demographics_data.zipcode_education`
            ) USING(fk_zipcodes)
        JOIN (
          SELECT
            CAST(pid AS STRING) AS fk_zipcodes
          FROM
            `{{ var.value.storage_project_id }}.{{ params['area_geometries_dataset'] }}.{{ params['zipcodes_table'] }}`
          WHERE state IN ({{ dag_run.conf['states'] }})
        ) USING(fk_zipcodes)
        )
    ) UNPIVOT(
      percentage FOR statistic IN (
        white,
        black_or_african_american,
        american_indian_or_alaskan_native,
        asian,
        native_hawaiian_and_other_pacific_islander,
        other_race,
        two_or_more_races,
        pop_0_10,
        pop_10_to_19,
        pop_20_to_29,
        pop_30_to_39,
        pop_40_to_49,
        pop_50_to_59,
        pop_60_to_69,
        pop_70_to_79,
        pop_80_plus,
        income_000k_010k,
        income_010k_015k,
        income_015k_025k,
        income_025k_035k,
        income_035k_050k,
        income_050k_075k,
        income_075k_100k,
        income_100k_150k,
        income_150k_200k,
        income_200k_plus,
        education_0_no_high_school,
        education_1_high_school_grad,
        education_2_asociate_degree,
        education_3_bachelor_degree,
        education_4_grad_or_prof_degree
      )
    )
),
agg_demos_met AS (
  SELECT
    source,
    percentage,
    statistic,
  FROM
    (
      SELECT
        "POI visits" AS source,
        SUM(known_demos_score) AS population_count,
        SUM(white * known_demos_score) / SUM(known_demos_score) AS white,
        SUM(black_or_african_american * known_demos_score) / SUM(known_demos_score) AS black_or_african_american,
        SUM(
          american_indian_or_alaskan_native * known_demos_score
        ) / SUM(known_demos_score) AS american_indian_or_alaskan_native,
        SUM(asian * known_demos_score) / SUM(known_demos_score) AS asian,
        SUM(
          native_hawaiian_and_other_pacific_islander * known_demos_score
        ) / SUM(known_demos_score) AS native_hawaiian_and_other_pacific_islander,
        SUM(other_race * known_demos_score) / SUM(known_demos_score) AS other_race,
        SUM(two_or_more_races * known_demos_score) / SUM(known_demos_score) AS two_or_more_races,
        SUM((pAge_0_4 + pAge_5_9) * known_demos_score) / SUM(known_demos_score) AS pop_0_10,
        SUM(
          (pAge_10_14 + pAge_15_17 + pAge_18_19) * known_demos_score
        ) / SUM(known_demos_score) AS pop_10_to_19,
        SUM(
          (pAge_20 + pAge_21 + pAge_22_24 + pAge_25_29) * known_demos_score
        ) / SUM(known_demos_score) AS pop_20_to_29,
        SUM((pAge_30_34 + pAge_35_39) * known_demos_score) / SUM(known_demos_score) AS pop_30_to_39,
        SUM((pAge_40_44 + pAge_45_49) * known_demos_score) / SUM(known_demos_score) AS pop_40_to_49,
        SUM((pAge_50_54 + pAge_55_59) * known_demos_score) / SUM(known_demos_score) AS pop_50_to_59,
        SUM(
          (
            pAge_60_61 + pAge_62_64 + pAge_65_66 + pAge_67_69
          ) * known_demos_score
        ) / SUM(known_demos_score) AS pop_60_to_69,
        SUM((pAge_70_74 + pAge_75_79) * known_demos_score) / SUM(known_demos_score) AS pop_70_to_79,
        SUM((pAge_80_84 + pAge_85pl) * known_demos_score) / SUM(known_demos_score) AS pop_80_plus,
        SUM((fk_incomes_01) * known_demos_score) / SUM(known_demos_score) AS income_000k_010k,
        SUM((fk_incomes_02) * known_demos_score) / SUM(known_demos_score) AS income_010k_015k,
        SUM(
          (fk_incomes_03 + fk_incomes_04) * known_demos_score
        ) / SUM(known_demos_score) AS income_015k_025k,
        SUM(
          (fk_incomes_05 + fk_incomes_06) * known_demos_score
        ) / SUM(known_demos_score) AS income_025k_035k,
        SUM(
          (fk_incomes_07 + fk_incomes_08 + fk_incomes_09) * known_demos_score
        ) / SUM(known_demos_score) AS income_035k_050k,
        SUM(
          (fk_incomes_10 + fk_incomes_11) * known_demos_score
        ) / SUM(known_demos_score) AS income_050k_075k,
        SUM((fk_incomes_12) * known_demos_score) / SUM(known_demos_score) AS income_075k_100k,
        SUM(
          (fk_incomes_13 + fk_incomes_14) * known_demos_score
        ) / SUM(known_demos_score) AS income_100k_150k,
        SUM((fk_incomes_15) * known_demos_score) / SUM(known_demos_score) AS income_150k_200k,
        SUM((fk_incomes_16) * known_demos_score) / SUM(known_demos_score) AS income_200k_plus,
        SUM(
          (
            pEdAttain_Non + pEdAttain_Nurs_4thGrd + pEdAttain_5_6thGrd + pEdAttain_7_8thGrd + pEdAttain_9thGrd + pEdAttain_10thGrd + pEdAttain_11thGrd + pEdAttain_12thGrd
          ) * known_demos_score
        ) / SUM(known_demos_score) AS education_0_no_high_school,
        SUM(
          (
            pEdAttain_HighSch_Grad + pEdAttain_College_u1yr + pEdAttain_College_1pl_nodegree
          ) * known_demos_score
        ) / SUM(known_demos_score) AS education_1_high_school_grad,
        SUM((pEdAttain_AssocDegree) * known_demos_score) / SUM(known_demos_score) AS education_2_asociate_degree,
        SUM((pEdAttain_BachDegree) * known_demos_score) / SUM(known_demos_score) AS education_3_bachelor_degree,
        SUM(
          (
            pEdAttain_MastDegree + pEdAttain_ProfSchDegree + pEdAttain_DoctDegree
          ) * known_demos_score
        ) / SUM(known_demos_score) AS education_4_grad_or_prof_degree,
      FROM
        `storage-dev-olvin-com.demographics.place_v2`
      JOIN (SELECT pid AS fk_sgplaces, region AS state
      FROM `{{ var.value.storage_project_id }}.{{ params['places_dataset'] }}.{{ params['places_table'] }}`)
      USING(fk_sgplaces)

          WHERE state IN ({{ dag_run.conf['states'] }})
    ) UNPIVOT(
      percentage FOR statistic IN (
        white,
        black_or_african_american,
        american_indian_or_alaskan_native,
        asian,
        native_hawaiian_and_other_pacific_islander,
        other_race,
        two_or_more_races,
        pop_0_10,
        pop_10_to_19,
        pop_20_to_29,
        pop_30_to_39,
        pop_40_to_49,
        pop_50_to_59,
        pop_60_to_69,
        pop_70_to_79,
        pop_80_plus,
        income_000k_010k,
        income_010k_015k,
        income_015k_025k,
        income_025k_035k,
        income_035k_050k,
        income_050k_075k,
        income_075k_100k,
        income_100k_150k,
        income_150k_200k,
        income_200k_plus,
        education_0_no_high_school,
        education_1_high_school_grad,
        education_2_asociate_degree,
        education_3_bachelor_degree,
        education_4_grad_or_prof_degree
      )
    )
),
device_demos_met AS (
  SELECT
    source,
    percentage,
    statistic
  FROM
    (
      SELECT
        "Olvin Sample" AS source,
        SUM(white * weight) / SUM(weight) AS white,
        SUM(black_or_african_american * weight) / SUM(weight) AS black_or_african_american,
        SUM(
          american_indian_or_alaskan_native * weight
        ) / SUM(weight) AS american_indian_or_alaskan_native,
        SUM(asian * weight) / SUM(weight) AS asian,
        SUM(
          native_hawaiian_and_other_pacific_islander * weight
        ) / SUM(weight) AS native_hawaiian_and_other_pacific_islander,
        SUM(other_race * weight) / SUM(weight) AS other_race,
        SUM(two_or_more_races * weight) / SUM(weight) AS two_or_more_races,
        SUM((pAge_0_4 + pAge_5_9) * weight) / SUM(weight) AS pop_0_10,
        SUM((pAge_10_14 + pAge_15_17 + pAge_18_19) * weight) / SUM(weight) AS pop_10_to_19,
        SUM(
          (pAge_20 + pAge_21 + pAge_22_24 + pAge_25_29) * weight
        ) / SUM(weight) AS pop_20_to_29,
        SUM((pAge_30_34 + pAge_35_39) * weight) / SUM(weight) AS pop_30_to_39,
        SUM((pAge_40_44 + pAge_45_49) * weight) / SUM(weight) AS pop_40_to_49,
        SUM((pAge_50_54 + pAge_55_59) * weight) / SUM(weight) AS pop_50_to_59,
        SUM(
          (
            pAge_60_61 + pAge_62_64 + pAge_65_66 + pAge_67_69
          ) * weight
        ) / SUM(weight) AS pop_60_to_69,
        SUM((pAge_70_74 + pAge_75_79) * weight) / SUM(weight) AS pop_70_to_79,
        SUM((pAge_80_84 + pAge_85pl) * weight) / SUM(weight) AS pop_80_plus,
        SUM((pm_uUSD10000) * weight) / SUM(weight) AS income_000k_010k,
        SUM((pm_USD10000_USD14999) * weight) / SUM(weight) AS income_010k_015k,
        SUM(
          (pm_USD15000_USD19999 + pm_USD20000_USD24999) * weight
        ) / SUM(weight) AS income_015k_025k,
        SUM(
          (pm_USD25000_USD29999 + pm_USD30000_USD34999) * weight
        ) / SUM(weight) AS income_025k_035k,
        SUM(
          (
            pm_USD35000_USD39999 + pm_USD40000_USD44999 + pm_USD45000_USD49999
          ) * weight
        ) / SUM(weight) AS income_035k_050k,
        SUM(
          (pm_USD50000_USD59999 + pm_USD60000_USD74999) * weight
        ) / SUM(weight) AS income_050k_075k,
        SUM((pm_USD75000_USD99999) * weight) / SUM(weight) AS income_075k_100k,
        SUM(
          (pm_USD100000_USD124999 + pm_USD125000_USD149999) * weight
        ) / SUM(weight) AS income_100k_150k,
        SUM((pm_USD150000_USD199999) * weight) / SUM(weight) AS income_150k_200k,
        SUM((pm_USD200000p) * weight) / SUM(weight) AS income_200k_plus,
        SUM(
          (
            pEdAttain_Non + pEdAttain_Nurs_4thGrd + pEdAttain_5_6thGrd + pEdAttain_7_8thGrd + pEdAttain_9thGrd + pEdAttain_10thGrd + pEdAttain_11thGrd + pEdAttain_12thGrd
          ) * weight
        ) / SUM(weight) AS education_0_no_high_school,
        SUM(
          (
            pEdAttain_HighSch_Grad + pEdAttain_College_u1yr + pEdAttain_College_1pl_nodegree
          ) * weight
        ) / SUM(weight) AS education_1_high_school_grad,
        SUM((pEdAttain_AssocDegree) * weight) / SUM(weight) AS education_2_asociate_degree,
        SUM((pEdAttain_BachDegree) * weight) / SUM(weight) AS education_3_bachelor_degree,
        SUM(
          (
            pEdAttain_MastDegree + pEdAttain_ProfSchDegree + pEdAttain_DoctDegree
          ) * weight
        ) / SUM(weight) AS education_4_grad_or_prof_degree,
    SUM(IF(CAST(CAMEO_INTL - FLOOR(CAMEO_INTL/10)*10 AS INT64) = 1, 100, 0) * weight) / SUM(weight) AS fk_life_stage_1,
    SUM(IF(CAST(CAMEO_INTL - FLOOR(CAMEO_INTL/10)*10 AS INT64) = 2, 100, 0) * weight) / SUM(weight) AS fk_life_stage_2,
    SUM(IF(CAST(CAMEO_INTL - FLOOR(CAMEO_INTL/10)*10 AS INT64) = 3, 100, 0) * weight) / SUM(weight) AS fk_life_stage_3,
    SUM(IF(CAST(CAMEO_INTL - FLOOR(CAMEO_INTL/10)*10 AS INT64) = 4, 100, 0) * weight) / SUM(weight) AS fk_life_stage_4,
    SUM(IF(CAST(CAMEO_INTL - FLOOR(CAMEO_INTL/10)*10 AS INT64) = 5, 100, 0) * weight) / SUM(weight) AS fk_life_stage_5
      FROM
        (SELECT * FROM `storage-dev-olvin-com.{{ params['zipcodes_dataset'] }}.*`
        WHERE local_date >= "{{ dag_run.conf['start_date_string'] }}" AND local_date < "{{ dag_run.conf['end_date_string'] }}")
        JOIN (
          SELECT
            *
          EXCEPT(point)
          FROM
            `storage-dev-olvin-com.static_demographics_data.zipcode_demographics` AS zipcode_table
            LEFT JOIN `storage-dev-olvin-com.static_demographics_data.cameo_categories_processed` USING(CAMEO_USA)
            LEFT JOIN (
              SELECT
                zip AS fk_zipcodes,
                population_count,
                white / population_count * 100 AS white,
                black_or_african_american / population_count * 100 AS black_or_african_american,
                american_indian_or_alaskan_native / population_count * 100 AS american_indian_or_alaskan_native,
                asian / population_count * 100 AS asian,
                native_hawaiian_and_other_pacific_islander / population_count * 100 AS native_hawaiian_and_other_pacific_islander,
                other_race / population_count * 100 AS other_race,
                two_or_more_races / population_count * 100 AS two_or_more_races,
              FROM
                `storage-dev-olvin-com.static_demographics_data.census_zipcodes`
              WHERE
                population_count > 0 AND state IN ({{ dag_run.conf['states'] }})
            ) AS race_table USING(fk_zipcodes)
        ) USING(zip_id, fk_zipcodes)
        JOIN (
          SELECT
            pid AS fk_zipcodes
          FROM
            `{{ var.value.storage_project_id }}.{{ params['area_geometries_dataset'] }}.{{ params['zipcodes_table'] }}`
          WHERE state IN ({{ dag_run.conf['states'] }})
        ) USING(fk_zipcodes)
    ) UNPIVOT(
      percentage FOR statistic IN (
        white,
        black_or_african_american,
        american_indian_or_alaskan_native,
        asian,
        native_hawaiian_and_other_pacific_islander,
        other_race,
        two_or_more_races,
        pop_0_10,
        pop_10_to_19,
        pop_20_to_29,
        pop_30_to_39,
        pop_40_to_49,
        pop_50_to_59,
        pop_60_to_69,
        pop_70_to_79,
        pop_80_plus,
        income_000k_010k,
        income_010k_015k,
        income_015k_025k,
        income_025k_035k,
        income_035k_050k,
        income_050k_075k,
        income_075k_100k,
        income_100k_150k,
        income_150k_200k,
        income_200k_plus,
        education_0_no_high_school,
        education_1_high_school_grad,
        education_2_asociate_degree,
        education_3_bachelor_degree,
        education_4_grad_or_prof_degree,
    fk_life_stage_1,
    fk_life_stage_2,
    fk_life_stage_3,
    fk_life_stage_4,
    fk_life_stage_5
      )
    )
)
SELECT
  *,
  CASE
    WHEN statistic IN (
      "white",
      "black_or_african_american",
      "american_indian_or_alaskan_native",
      "asian",
      "native_hawaiian_and_other_pacific_islander",
      "other_race",
      "two_or_more_races"
    ) THEN "race"
    WHEN statistic IN (
      "pop_0_10",
      "pop_10_to_19",
      "pop_20_to_29",
      "pop_30_to_39",
      "pop_40_to_49",
      "pop_50_to_59",
      "pop_60_to_69",
      "pop_70_to_79",
      "pop_80_plus"
    ) THEN "age"
    WHEN statistic IN (
      "income_000k_010k",
      "income_010k_015k",
      "income_015k_025k",
      "income_025k_035k",
      "income_035k_050k",
      "income_050k_075k",
      "income_075k_100k",
      "income_100k_150k",
      "income_150k_200k",
      "income_200k_plus"
    ) THEN "income"
    WHEN statistic IN (
      "education_0_no_high_school",
      "education_1_high_school_grad",
      "education_2_asociate_degree",
      "education_3_bachelor_degree",
      "education_4_grad_or_prof_degree"
    ) THEN "education"
    WHEN statistic IN (
      "fk_life_stage_1",
      "fk_life_stage_2",
      "fk_life_stage_3",
      "fk_life_stage_4",
      "fk_life_stage_5"
    ) THEN "life_stage"
    ELSE "unknown"
  END AS stats_group
FROM
  (
    SELECT
      *
    FROM
      geospatial_census_join
    UNION ALL
    SELECT
      *
    FROM
      agg_demos_met
    UNION ALL
    SELECT
      *
    FROM
      device_demos_met
  )