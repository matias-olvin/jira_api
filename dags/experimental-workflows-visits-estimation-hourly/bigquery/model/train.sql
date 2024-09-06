
CREATE OR REPLACE MODEL
  `storage-dev-olvin-com.visits_estimation.adjustments_hourly_regression_model`

OPTIONS
  (model_type='linear_reg',
  input_label_cols=['output_column']
  ) AS

select
*
from 
  `storage-dev-olvin-com.visits_estimation.adjustments_hourly_model_input`
where output_column is not null
;

create or replace table
    `storage-dev-olvin-com.visits_estimation.adjustments_hourly_supervised_weights`
as

SELECT
  processed_input,
  weight
FROM
  ML.WEIGHTS(MODEL
    `storage-dev-olvin-com.visits_estimation.adjustments_hourly_regression_model`

    )
