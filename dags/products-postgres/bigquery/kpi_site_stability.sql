-- DECLARE current_month DATE DEFAULT DATE_TRUNC(DATE_ADD('{{ ds }}', INTERVAL 1 MONTH), MONTH);  -- TO BE AUTOMATED
-- DECLARE previous_month DATE DEFAULT DATE_TRUNC('{{ ds }}', MONTH);  -- TO BE AUTOMATED


with 
    current_month_rankings as (
        SELECT
            fk_sgplaces,
            row_number() over(order by visits desc) AS total_ranking_current_month,
        FROM 
            `{{ params['project'] }}.{{ params['postgres_batch_dataset'] }}.SGPlaceMonthlyVisitsRaw` monthly_visits -- This table is the table we sent to postgres in the current month
        WHERE 
            local_date = current_month
    ),
    previous_month_rankings as (
        SELECT
            fk_sgplaces,
            row_number() over(order by visits desc) AS total_ranking_previous_month,
        FROM 
            `{{ params['project'] }}.{{ params['postgres_dataset'] }}.SGPlaceMonthlyVisitsRaw_prev` monthly_visits -- This table should be the  table that we sent the previous month to postgres
        WHERE 
            local_date = previous_month
    ),

    joining_tables as (
        SELECT 
            *,
            row_number() over(order by rand()) / count(*) over()  as rd_number
        FROM 
            current_month_rankings
        INNER JOIN 
            previous_month_rankings USING(fk_sgplaces)
    ),
    joining_tables_sample as (
        SELECT 
            *
        FROM 
            joining_tables
        WHERE 
            rd_number < 0.01
    ),

    kendall_tau as (
        SELECT --Kendall's rank correlation sample estimate τ
            ((SUM(CASE WHEN (i.total_ranking_current_month < j.total_ranking_current_month AND i.total_ranking_previous_month < j.total_ranking_previous_month) OR (i.total_ranking_current_month > j.total_ranking_current_month AND i.total_ranking_previous_month > j.total_ranking_previous_month) THEN 1 ELSE 0 END)) -- concordant
            - SUM(CASE WHEN (i.total_ranking_current_month < j.total_ranking_current_month AND i.total_ranking_previous_month > j.total_ranking_previous_month) OR (i.total_ranking_current_month > j.total_ranking_current_month AND i.total_ranking_previous_month < j.total_ranking_previous_month) THEN 1 ELSE 0 END)) -- discordant
            /COUNT(*) AS Tau
        FROM  
            joining_tables_sample i 
        CROSS JOIN 
            joining_tables_sample j
        WHERE 
            i.total_ranking_current_month<>j.total_ranking_current_month
    )

select *
from kendall_tau