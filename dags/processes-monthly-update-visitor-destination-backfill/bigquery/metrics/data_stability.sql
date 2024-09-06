with 
    current_month_rankings as (
        SELECT
            fk_sgplaces,
            fk_sgbrands,
            row_number() over(partition by fk_sgplaces order by shared_devices desc) AS total_ranking_current_month,
        FROM 
            `storage-prod-olvin-com.visitor_brand_destinations.observed_connections`  -- This table is the table we sent to postgres in the current month
        -- WHERE 
        --     local_date = current_month

    ),
    previous_month_rankings as (
        SELECT
            fk_sgplaces,
            fk_sgbrands,
            row_number() over(partition by fk_sgplaces order by shared_devices desc) AS total_ranking_previous_month,
        FROM 
            `storage-prod-olvin-com.visitor_brand_destinations.observed_connections`  -- This table should be the  table that we sent the previous month to postgres
        -- WHERE 
        --     local_date = previous_month
    ),

    joining_tables as (
        SELECT 
            *,
        FROM 
            current_month_rankings
        INNER JOIN 
            previous_month_rankings USING(fk_sgplaces, fk_sgbrands)
    ),
    select_random as (
        select fk_sgplaces
        from
        (select fk_sgplaces, row_number() over (order by rand()) rd_nb
        from
        (select distinct fk_sgplaces
        from joining_tables))
        where rd_nb = 1
    ),

    joining_tables_sample as (
        SELECT 
            *
        FROM 
            joining_tables
        inner join  select_random using (fk_sgplaces)
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