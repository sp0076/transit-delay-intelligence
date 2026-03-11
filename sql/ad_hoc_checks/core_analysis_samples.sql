-- Core SQL samples for portfolio/review
-- Run after build scripts have populated marts.

-- 1) Which routes have the highest average delay?
SELECT
  route_id,
  avg_delay_minutes,
  p95_delay_minutes,
  on_time_rate,
  stop_event_count
FROM marts.route_performance
ORDER BY avg_delay_minutes DESC;

-- 2) Which stops are most delay-prone?
SELECT
  stop_name,
  route_id,
  avg_delay_minutes,
  median_delay_minutes,
  on_time_rate,
  stop_event_count
FROM marts.stop_performance
ORDER BY avg_delay_minutes DESC
LIMIT 25;

-- 3) Which hour/day combinations are most unreliable?
SELECT
  day_of_week,
  hour_of_day,
  route_id,
  avg_delay_minutes,
  delay_probability_pct,
  stop_event_count
FROM marts.hour_day_reliability
ORDER BY avg_delay_minutes DESC, delay_probability_pct DESC
LIMIT 30;

-- 4) Are delays worse in event windows?
SELECT
  in_event_window,
  event_type,
  avg_delay_minutes,
  delay_probability_pct,
  stop_event_count
FROM marts.event_window_impact
ORDER BY in_event_window DESC, avg_delay_minutes DESC;

-- 5) What portion of delays are associated with disruptions?
SELECT
  in_alert_window,
  alert_type,
  pct_rows_associated_with_alerts,
  avg_delay_minutes,
  delay_probability_pct,
  stop_event_count
FROM marts.disruption_association
ORDER BY in_alert_window DESC, pct_rows_associated_with_alerts DESC;

-- 6) Next-24h high-risk periods
SELECT
  forecast_hour_local_ts,
  route_id,
  projected_delay_minutes,
  projected_delay_probability_pct,
  risk_level,
  precipitation_mm,
  windspeed_kmh
FROM marts.risk_forecast_next_24h
ORDER BY projected_delay_minutes DESC, forecast_hour_local_ts ASC
LIMIT 40;
