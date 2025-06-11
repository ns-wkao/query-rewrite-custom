SELECT
    ns_tenant_id,
    app,
    organization_unit,
    SUM("count") AS count,
    SUM(numbytes) AS numbytes
FROM "redshift_poc_iceberg"."page_v3"
WHERE ns_tenant_id = 2683
    AND timestamp >= TIMESTAMP '2025-01-01 15:23:00'
    AND timestamp < TIMESTAMP '2025-01-01 15:27:00'
GROUP BY ns_tenant_id, app, organization_unit