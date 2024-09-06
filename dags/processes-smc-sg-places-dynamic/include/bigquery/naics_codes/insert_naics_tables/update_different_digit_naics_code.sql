DELETE `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['different_digit_naics_code_table'] }}`
WHERE
    six_digit_code IN (
        SELECT
            six_digit_code
        FROM
            `{{ var.value.env_project }}.{{ params['sg_base_tables_staging_dataset'] }}.{{ params['different_digit_naics_code_table'] }}_staging`
    );

INSERT INTO
    `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['different_digit_naics_code_table'] }}`
SELECT
    two_digit_code,
    two_digit_title,
    four_digit_code,
    four_digit_title,
    six_digit_code,
    sub_category
FROM
    `{{ var.value.env_project }}.{{ params['sg_base_tables_staging_dataset'] }}.{{ params['different_digit_naics_code_table'] }}_staging`;