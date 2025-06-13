SELECT
  date_format(
    date_trunc('hour', TIMESTAMP),
    '%Y-%m-%d %H:00:00'
  ) formatted_hour,
  ns_tenant_id,
  SUM("count") total_count
FROM
  redshift_poc_iceberg.alert_event_hourly_sum
WHERE
  (action = 'alert')
GROUP BY
  1,
  ns_tenant_id
ORDER BY
  formatted_hour ASC