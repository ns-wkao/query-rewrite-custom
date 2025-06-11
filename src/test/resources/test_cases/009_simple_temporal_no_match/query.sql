SELECT 
    date_trunc('minute', timestamp) as minute,
    ns_tenant_id,
    alert_type,
    SUM("count") as total_count
FROM "redshift_poc_iceberg"."alert_v3"
WHERE action = 'alert'
GROUP BY 
    date_trunc('minute', timestamp),
    ns_tenant_id, 
    alert_type
ORDER BY minute