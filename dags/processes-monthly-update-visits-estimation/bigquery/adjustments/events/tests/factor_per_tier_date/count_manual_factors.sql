SELECT CASE WHEN num_1s/num_factors > 0.02 THEN TRUE                -- 1 is certainly manual
            ELSE CASE WHEN num_0s/num_factors > 0 THEN TRUE       -- some 0s are manual, but most are due to closings
                      ELSE FALSE
                      END
            END
       AS error
FROM (
    SELECT COUNT(*) AS num_factors, COUNTIF(factor = 0) AS num_0s, COUNTIF(factor = 1) AS num_1s
    FROM
        `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['event_factor_tier_table'] }}`
)