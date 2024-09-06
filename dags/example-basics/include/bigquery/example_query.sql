SELECT
  DATE("{{ params['date'] }}") AS start_date
  , DATE_ADD("{{ params['date'] }}", INTERVAL 1 DAY) AS end_date
