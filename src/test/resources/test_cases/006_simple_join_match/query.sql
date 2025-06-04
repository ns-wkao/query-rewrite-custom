SELECT
  a.ns_tenant_id,
  a.alert_type,
  d.policy_description,
  SUM(a."count") AS total_count
FROM
  "redshift_poc_iceberg"."alert_v3" AS a
JOIN
  "redshift_poc_iceberg"."policy_details" AS d
  ON a.policy = d.policy_name
WHERE a.action = 'alert'
GROUP BY
  a.ns_tenant_id,
  a.alert_type,
  d.policy_description