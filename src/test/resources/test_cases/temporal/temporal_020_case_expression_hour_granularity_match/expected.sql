SELECT
  (
    CASE
      WHEN (action = 'block') THEN date_trunc('hour', TIMESTAMP)
      ELSE date_trunc('day', TIMESTAMP)
    END
  ) time_bucket,
  ns_tenant_id,
  alert_type,
  SUM("count") total_count
FROM
  redshift_poc_iceberg.alert_event_hourly_sum
WHERE
  (ns_tenant_id = 2683)
GROUP BY
  1,
  ns_tenant_id,
  alert_type
ORDER BY
  time_bucket ASC