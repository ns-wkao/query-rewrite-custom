SELECT
  date_trunc('month', TIMESTAMP) AS month,
  ns_id,
  organization_unit,
  appcategory,
  SUM("count") AS total_count
FROM
  "redshift_poc_iceberg"."alert_v3"
GROUP BY
  date_trunc('month', TIMESTAMP),
  ns_id,
  organization_unit,
  appcategory
ORDER BY
  month