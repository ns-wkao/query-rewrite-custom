SELECT
  ns_tenant_id,
  app,
  organization_unit,
  SUM("count") count,
  SUM(numbytes) numbytes
FROM
  redshift_poc_iceberg.page_event_30min_sum
WHERE
  (
    (ns_tenant_id = 2683)
    AND (TIMESTAMP >= TIMESTAMP '2025-01-01 15:30:00')
    AND (TIMESTAMP < TIMESTAMP '2025-01-01 16:00:00')
  )
GROUP BY
  ns_tenant_id,
  app,
  organization_unit