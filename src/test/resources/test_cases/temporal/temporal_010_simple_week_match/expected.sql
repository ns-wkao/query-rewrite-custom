SELECT
  date_trunc('week', TIMESTAMP) week,
  ns_tenant_id,
  alert_type,
  SUM("count") total_count
FROM
  redshift_poc_iceberg.alert_event_daily_sum_detailed
WHERE
  (action = 'alert')
GROUP BY
  date_trunc('week', TIMESTAMP),
  ns_tenant_id,
  alert_type
ORDER BY
  week ASC