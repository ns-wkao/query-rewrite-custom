SELECT 
    date_trunc('week', timestamp) as week,
    ns_tenant_id,
    alert_type,
    SUM("count") as total_count
FROM "redshift_poc_iceberg"."alert_v3"
WHERE action = 'alert'
GROUP BY 
    date_trunc('week', timestamp),
    ns_tenant_id, 
    alert_type
ORDER BY week