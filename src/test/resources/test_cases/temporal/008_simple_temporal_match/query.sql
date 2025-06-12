-- Test: MV with daily granularity can satisfy query with daily granularity (exact match)
-- Expected: MATCH - identical temporal granularity
SELECT 
    date_trunc('day', timestamp) as day,
    ns_tenant_id,
    alert_type,
    SUM("count") as total_count
FROM "redshift_poc_iceberg"."alert_v3"
WHERE action = 'alert'
GROUP BY 
    date_trunc('day', timestamp),
    ns_tenant_id, 
    alert_type
ORDER BY day