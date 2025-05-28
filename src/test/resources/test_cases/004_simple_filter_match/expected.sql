SELECT
  ns_tenant_id,
  alert_type,
  SUM("count")
FROM
  redshift_poc_iceberg.alert_event_daily_sum_detailed
WHERE
  (action = 'block')
GROUP BY
  ns_tenant_id,
  alert_type