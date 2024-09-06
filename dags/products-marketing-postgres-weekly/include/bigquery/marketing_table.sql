INSERT INTO `{{ var.value.env_project }}.{{ params['postgres_metrics_dataset'] }}.{{ params['marketing_table'] }}` 

WITH 

get_visits AS (
    SELECT 
        visits.*,
        SGBrandRaw.pid,
        SGBrandRaw.name,
        SGBrandRaw.top_category,
        SGBrandRaw.sub_category
    FROM
        `{{ params['olvin_almanac_project'] }}.{{ params['postgres_rt_dataset'] }}.{{ params['sgplacedailyvisitsraw_table'] }}` visits
        LEFT JOIN `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}` SGPlaceRaw ON fk_sgplaces = SGPlaceRaw.pid
        LEFT JOIN `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgbrandraw_table'] }}` SGBrandRaw ON fk_sgbrands = SGBrandRaw.pid
    WHERE SGBrandRaw.pid IS NOT NULL    
    AND (closing_date IS null OR closing_date='2100-01-01')
),

daily_visits AS (
    SELECT
        DATE_ADD(local_date, INTERVAL row_number DAY) AS local_date,
        CAST(visits AS FLOAT64) AS visits,
        fk_sgplaces,
        pid,
        name,
        top_category,
        sub_category
    FROM (
        SELECT
            local_date,
            fk_sgplaces, 
            JSON_EXTRACT_ARRAY(visits) AS visit_array,
            pid,
            name,
            top_category,
            sub_category
        FROM get_visits
    )
    CROSS JOIN UNNEST(visit_array) AS visits
    WITH OFFSET AS row_number
    ORDER BY local_date, fk_sgplaces, row_number
),

weekly_visits AS (
    SELECT
        pid,
        name,
        top_category,
        sub_category,
        DATE_TRUNC(local_date, WEEK(MONDAY)) AS week_start,
        EXTRACT(YEAR FROM local_date) AS year,
        EXTRACT(WEEK FROM local_date) AS week,
        SUM(visits) AS total_visits
    FROM daily_visits
    GROUP BY pid, name, top_category, sub_category, week_start, year, week
),

monthly_visits AS (
    SELECT
        pid,
        name,
        top_category,
        sub_category,
        DATE_TRUNC(local_date, MONTH) AS month_start,
        SUM(visits) AS total_visits
    FROM daily_visits
    GROUP BY pid, name, top_category, sub_category, month_start
),

quarterly_visits AS (
    SELECT
        pid,
        name,
        top_category,
        sub_category,
        DATE_TRUNC(local_date, QUARTER) AS quarter_start,
        SUM(visits) AS total_visits
    FROM daily_visits
    GROUP BY pid, name, top_category, sub_category, quarter_start
),

-- Aggregated visits for the periods
last_week AS (
    SELECT
        pid,
        name,
        top_category,
        sub_category,
        SUM(total_visits) AS last_week_visits
    FROM weekly_visits
    WHERE week_start = DATE_SUB(DATE_TRUNC(CAST('{{ next_ds }}' AS DATE), WEEK(MONDAY)), INTERVAL 1 WEEK)
    GROUP BY pid, name, top_category, sub_category
),

same_week_last_year AS (
    SELECT
        pid,
        name,
        top_category,
        sub_category,
        SUM(total_visits) AS same_week_last_year_visits
    FROM weekly_visits
    WHERE year = EXTRACT(YEAR FROM DATE_SUB(CAST('{{ next_ds }}' AS DATE), INTERVAL 1 YEAR))
      AND week = EXTRACT(WEEK FROM CAST('{{ next_ds }}' AS DATE))
    GROUP BY pid, name, top_category, sub_category
),

previous_week AS (
    SELECT
        pid,
        name,
        top_category,
        sub_category,
        SUM(total_visits) AS previous_week_visits
    FROM weekly_visits
    WHERE week_start = DATE_SUB(DATE_TRUNC(CAST('{{ next_ds }}' AS DATE), WEEK(MONDAY)), INTERVAL 2 WEEK)
    GROUP BY pid, name, top_category, sub_category
),

month_to_date AS (
    SELECT
        pid,
        name,
        top_category,
        sub_category,
        SUM(visits) AS month_to_date_visits
    FROM daily_visits
    WHERE local_date >= DATE_TRUNC(CAST('{{ next_ds }}' AS DATE), MONTH) AND local_date < CAST('{{ next_ds }}' AS DATE)
    GROUP BY pid, name, top_category, sub_category
),

same_month_last_year AS (
    SELECT
        pid,
        name,
        top_category,
        sub_category,
        SUM(visits) AS same_month_last_year_visits
    FROM daily_visits
    WHERE local_date >= DATE_TRUNC(DATE_SUB(CAST('{{ next_ds }}' AS DATE), INTERVAL 1 YEAR), MONTH)
        AND local_date < DATE_ADD(DATE_TRUNC(DATE_SUB(CAST('{{ next_ds }}' AS DATE), INTERVAL 1 YEAR), MONTH), INTERVAL EXTRACT(DAY FROM CAST('{{ next_ds }}' AS DATE)) - 1 DAY)
    GROUP BY pid, name, top_category, sub_category
),

first_x_days_previous_month AS (
    SELECT
        pid,
        name,
        top_category,
        sub_category,
        SUM(visits) AS first_x_days_previous_month_visits
    FROM daily_visits
    WHERE local_date >= DATE_TRUNC(DATE_SUB(CAST('{{ next_ds }}' AS DATE), INTERVAL 1 MONTH), MONTH)
        AND local_date < DATE_ADD(DATE_TRUNC(DATE_SUB(CAST('{{ next_ds }}' AS DATE), INTERVAL 1 MONTH), MONTH), INTERVAL EXTRACT(DAY FROM CAST('{{ next_ds }}' AS DATE)) - 1 DAY)
    GROUP BY pid, name, top_category, sub_category
),

quarter_to_date AS (
    SELECT
        pid,
        name,
        top_category,
        sub_category,
        SUM(visits) AS quarter_to_date_visits
    FROM daily_visits
    WHERE local_date >= DATE_TRUNC(CAST('{{ next_ds }}' AS DATE), QUARTER) AND local_date < CAST('{{ next_ds }}' AS DATE)
    GROUP BY pid, name, top_category, sub_category
),

same_quarter_last_year AS (
    SELECT
        pid,
        name,
        top_category,
        sub_category,
        SUM(visits) AS same_quarter_last_year_visits
    FROM daily_visits
    WHERE local_date >= DATE_TRUNC(DATE_SUB(CAST('{{ next_ds }}' AS DATE), INTERVAL 1 YEAR), QUARTER)
        AND local_date < DATE_ADD(DATE_TRUNC(DATE_SUB(CAST('{{ next_ds }}' AS DATE), INTERVAL 1 YEAR), QUARTER), INTERVAL DATE_DIFF(CAST('{{ next_ds }}' AS DATE), DATE_TRUNC(CAST('{{ next_ds }}' AS DATE), QUARTER), DAY) DAY)
    GROUP BY pid, name, top_category, sub_category
),

last_month AS (
    SELECT
        pid,
        name,
        top_category,
        sub_category,
        SUM(total_visits) AS last_month_visits
    FROM monthly_visits
    WHERE month_start = DATE_TRUNC(DATE_SUB(CAST('{{ next_ds }}' AS DATE), INTERVAL 1 MONTH), MONTH)
    GROUP BY pid, name, top_category, sub_category
),

same_month_last_year_all_days AS (
    SELECT
        pid,
        name,
        top_category,
        sub_category,
        SUM(total_visits) AS same_month_last_year_all_days_visits
    FROM monthly_visits
    WHERE month_start = DATE_TRUNC(DATE_SUB(CAST('{{ next_ds }}' AS DATE), INTERVAL 1 YEAR), MONTH)
    GROUP BY pid, name, top_category, sub_category
),

last_quarter AS (
    SELECT
        pid,
        name,
        top_category,
        sub_category,
        SUM(total_visits) AS last_quarter_visits
    FROM quarterly_visits
    WHERE quarter_start = DATE_TRUNC(DATE_SUB(CAST('{{ next_ds }}' AS DATE), INTERVAL 1 QUARTER), QUARTER)
    GROUP BY pid, name, top_category, sub_category
),

same_quarter_last_year_all_days AS (
    SELECT
        pid,
        name,
        top_category,
        sub_category,
        SUM(total_visits) AS same_quarter_last_year_all_days_visits
    FROM quarterly_visits
    WHERE quarter_start = DATE_TRUNC(DATE_SUB(CAST('{{ next_ds }}' AS DATE), INTERVAL 1 YEAR), QUARTER)
    GROUP BY pid, name, top_category, sub_category
)

SELECT
    CAST('{{ next_ds }}' AS DATE) AS run_date,
    lw.pid AS fk_sgbrands,
    lw.name AS Brand_Name,
    lw.top_category AS Top_Category,
    lw.sub_category AS Sub_Category,
    lw.last_week_visits AS Last_Week_Visits,
    CASE
        WHEN sylw.same_week_last_year_visits = 0 THEN NULL
        ELSE ROUND(100 * (lw.last_week_visits - sylw.same_week_last_year_visits) / sylw.same_week_last_year_visits, 2)
    END AS YoY_Weekly_Change_Percentage,
    CASE
        WHEN pw.previous_week_visits = 0 THEN NULL
        ELSE ROUND(100 * (lw.last_week_visits - pw.previous_week_visits) / pw.previous_week_visits, 2)
    END AS WoW_Change_Percentage,
    mtd.month_to_date_visits AS Month_to_Date_Visits,
    CASE
        WHEN smly.same_month_last_year_visits = 0 THEN NULL
        ELSE ROUND(100 * (mtd.month_to_date_visits - smly.same_month_last_year_visits) / smly.same_month_last_year_visits, 2)
    END AS YoY_Monthly_Change_Percentage,
    CASE
        WHEN fxdpm.first_x_days_previous_month_visits = 0 THEN NULL
        ELSE ROUND(100 * (mtd.month_to_date_visits - fxdpm.first_x_days_previous_month_visits) / fxdpm.first_x_days_previous_month_visits, 2)
    END AS MoM_Change_Percentage,
    qtd.quarter_to_date_visits AS Quarter_to_Date_Visits,
    CASE
        WHEN sqly.same_quarter_last_year_visits = 0 THEN NULL
        ELSE ROUND(100 * (qtd.quarter_to_date_visits - sqly.same_quarter_last_year_visits) / sqly.same_quarter_last_year_visits, 2)
    END AS YoY_Quarterly_Change_Percentage,
    lm.last_month_visits AS Last_Month_Visits,
    CASE
        WHEN smlya.same_month_last_year_all_days_visits = 0 THEN NULL
        ELSE ROUND(100 * (lm.last_month_visits - smlya.same_month_last_year_all_days_visits) / smlya.same_month_last_year_all_days_visits, 2)
    END AS YoY_Last_Month_Change_Percentage,
    lq.last_quarter_visits AS Last_Quarter_Visits,
    CASE
        WHEN sqlya.same_quarter_last_year_all_days_visits = 0 THEN NULL
        ELSE ROUND(100 * (lq.last_quarter_visits - sqlya.same_quarter_last_year_all_days_visits) / sqlya.same_quarter_last_year_all_days_visits, 2)
    END AS YoY_Last_Quarter_Change_Percentage
FROM
    last_week lw
LEFT JOIN same_week_last_year sylw ON lw.pid = sylw.pid AND lw.name = sylw.name AND lw.top_category = sylw.top_category AND lw.sub_category = sylw.sub_category
LEFT JOIN previous_week pw ON lw.pid = pw.pid AND lw.name = pw.name AND lw.top_category = pw.top_category AND lw.sub_category = pw.sub_category
LEFT JOIN month_to_date mtd ON lw.pid = mtd.pid AND lw.name = mtd.name AND lw.top_category = mtd.top_category AND lw.sub_category = mtd.sub_category
LEFT JOIN same_month_last_year smly ON lw.pid = smly.pid AND lw.name = smly.name AND lw.top_category = smly.top_category AND lw.sub_category = smly.sub_category
LEFT JOIN first_x_days_previous_month fxdpm ON lw.pid = fxdpm.pid AND lw.name = fxdpm.name AND lw.top_category = fxdpm.top_category AND lw.sub_category = fxdpm.sub_category
LEFT JOIN quarter_to_date qtd ON lw.pid = qtd.pid AND lw.name = qtd.name AND lw.top_category = qtd.top_category AND lw.sub_category = qtd.sub_category
LEFT JOIN same_quarter_last_year sqly ON lw.pid = sqly.pid AND lw.name = sqly.name AND lw.top_category = sqly.top_category AND lw.sub_category = sqly.sub_category
LEFT JOIN last_month lm ON lw.pid = lm.pid AND lw.name = lm.name AND lw.top_category = lm.top_category AND lw.sub_category = lm.sub_category
LEFT JOIN same_month_last_year_all_days smlya ON lw.pid = smlya.pid AND lw.name = smlya.name AND lw.top_category = smlya.top_category AND lw.sub_category = smlya.sub_category
LEFT JOIN last_quarter lq ON lw.pid = lq.pid AND lw.name = lq.name AND lw.top_category = lq.top_category AND lw.sub_category = lq.sub_category
LEFT JOIN same_quarter_last_year_all_days sqlya ON lw.pid = sqlya.pid AND lw.name = sqlya.name AND lw.top_category = sqlya.top_category AND lw.sub_category = sqlya.sub_category
ORDER BY lw.pid;