SELECT
  ns_tenant_id,
  alert_type,
  action,
  SUM("count") count
FROM
  redshift_poc_iceberg.alert_event_hourly_sum
WHERE
  (
    (ns_tenant_id = 2683)
    AND (TIMESTAMP >= TIMESTAMP '2025-01-01 15:00:00')
    AND (TIMESTAMP < TIMESTAMP '2025-01-01 16:00:00')
  )
GROUP BY
  ns_tenant_id,
  alert_type,
  action