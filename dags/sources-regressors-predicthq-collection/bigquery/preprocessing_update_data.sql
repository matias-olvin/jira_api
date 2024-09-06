SELECT distinct
  id,
  duplicate_of_id,
  -- parent_event_id,--Not given in the API call data
  CAST(relevance AS string) AS relevance,
  updated,
  title,
  labels,
  category,
  description,
  start,
  a.end, --`end`
  predicted_end,
  timezone,
  duration,
  country,
  CAST(LEFT(split(location,',')[OFFSET(1)] ,CHAR_LENGTH(split(location,',')[OFFSET(1)])-1)AS float64) as lat,
  CAST(RIGHT(split(location,',')[OFFSET(0)] ,CHAR_LENGTH(split(location,',')[OFFSET(0)])-1)AS float64) as lon,
  geo,
  impact_patterns,
  -- venue_id,--Not given in the API call data
  -- venue_name,--Not given in the API call data
  -- venue_formatted_address,--Not given in the API call data
  scope,
  a.rank, --`rank`
  local_rank,
  CAST(aviation_rank AS INT64) AS aviation_rank ,
  CAST(phq_attendance AS INT64) AS phq_attendance,
  state,
  cancelled,
  postponed,
  deleted_reason,
  first_seen,
  place_hierarchies,
  private,
  brand_safe
FROM
  `{{ var.value.env_project }}.{{ params['regressors_staging_dataset'] }}.{{ params['daily_data_table'] }}` a
