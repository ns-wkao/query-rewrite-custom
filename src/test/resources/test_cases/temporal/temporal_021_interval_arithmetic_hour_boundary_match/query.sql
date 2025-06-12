-- date_add() with hour intervals - should match HOUR MV
SELECT 
    date_add('hour', 2, date_trunc('hour', timestamp)) AS time_bucket,
    ns_tenant_id,
    alert_type,
    SUM("count") AS total_count
FROM "redshift_poc_iceberg"."alert_v3"
WHERE action = 'alert'
GROUP BY 1, ns_tenant_id, alert_type
ORDER BY time_bucket