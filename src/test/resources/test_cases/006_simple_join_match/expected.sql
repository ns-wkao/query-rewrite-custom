SELECT
  a.ns_tenant_id,
  a.alert_type,
  d.policy_description,
  SUM(a."count") total_count
FROM
  (
    redshift_poc_iceberg.alert_event_daily_sum a
    INNER JOIN "redshift_poc_iceberg"."policy_details" d ON (a.policy = d.policy_name)
  )
WHERE
  (a.action = 'alert')
GROUP BY
  a.ns_tenant_id,
  a.alert_type,
  d.policy_description