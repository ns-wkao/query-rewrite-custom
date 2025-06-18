SELECT
  date_add('hour', 2, date_trunc('hour', TIMESTAMP)) time_bucket,
  ns_tenant_id,
  alert_type,
  SUM("count") total_count
FROM
  redshift_poc_iceberg.alert_event_hourly_sum
WHERE
  (action = 'alert')
GROUP BY
  1,
  ns_tenant_id,
  alert_type
ORDER BY
  time_bucket ASC