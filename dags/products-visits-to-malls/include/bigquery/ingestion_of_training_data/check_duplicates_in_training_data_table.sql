ASSERT (
    SELECT COUNT(*)
            FROM (
                SELECT 
                    COUNT(*) AS pid_count
                FROM 
                    `{{ var.value.env_project }}.{{ params['visits_to_malls_staging_dataset'] }}.{{ params['visits_to_malls_training_data_table'] }}`
                GROUP BY
                    pid
            )
            WHERE pid_count > 1
) = 0 AS "Duplicate pids found in {{ var.value.env_project }}.{{ params['visits_to_malls_staging_dataset'] }}.{{ params['visits_to_malls_training_data_table'] }}"