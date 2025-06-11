SELECT
  date_trunc('minute', TIMESTAMP) AS minute,
  ns_tenant_id,
  alert_type,
  SUM("count") AS total_count
FROM
  "redshift_poc_iceberg"."alert_v3"
WHERE
  action = 'alert'
GROUP BY
  date_trunc('minute', TIMESTAMP),
  ns_tenant_id,
  alert_type
ORDER BY
  minute