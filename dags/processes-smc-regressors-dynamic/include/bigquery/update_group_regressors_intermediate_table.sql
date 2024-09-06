SELECT 
  fk_sgplaces, 
  avg(visit_score) as avg_visit_score, 
  STDDEV(visit_score) as std_visit_score,
  COUNT(*) as total_visits
FROM 
    `{{ var.value.env_project }}.{{ params['poi_visits_scaled_table'] }}.{{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y') }}` 
GROUP BY fk_sgplaces