SELECT 
    date_add('minute', 30, date_trunc('hour', timestamp)) AS time_bucket,
    ns_tenant_id,
    app,
    SUM("count") AS total_count
FROM "redshift_poc_iceberg"."page_v3"
WHERE ns_tenant_id = 2683
GROUP BY 1, ns_tenant_id, app
ORDER BY time_bucket