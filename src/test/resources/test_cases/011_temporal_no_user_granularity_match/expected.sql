SELECT
  ns_tenant_id,
  alert_type,
  SUM("count") total_count
FROM
  redshift_poc_iceberg.alert_event_daily_sum_detailed
WHERE
  (action = 'alert')
GROUP BY
  ns_tenant_id,
  alert_type
ORDER BY
  ns_tenant_id ASC,
  alert_type ASC