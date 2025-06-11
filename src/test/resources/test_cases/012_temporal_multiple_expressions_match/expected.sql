SELECT
  date_trunc('day', TIMESTAMP) day,
  date_trunc('month', TIMESTAMP) month,
  ns_tenant_id,
  alert_type,
  SUM("count") total_count
FROM
  redshift_poc_iceberg.alert_event_daily_sum_detailed
WHERE
  (action = 'alert')
GROUP BY
  date_trunc('day', TIMESTAMP),
  date_trunc('month', TIMESTAMP),
  ns_tenant_id,
  alert_type
ORDER BY
  day ASC