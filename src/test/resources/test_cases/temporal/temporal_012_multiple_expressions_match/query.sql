-- Test: Query with multiple temporal expressions - should use finest granularity
-- Expected: MATCH - finest granularity (day) should be used for comparison
SELECT 
    date_trunc('day', timestamp) as day,
    date_trunc('month', timestamp) as month,
    ns_tenant_id,
    alert_type,
    SUM("count") as total_count
FROM "redshift_poc_iceberg"."alert_v3"
WHERE action = 'alert'
GROUP BY 
    date_trunc('day', timestamp),
    date_trunc('month', timestamp),
    ns_tenant_id, 
    alert_type
ORDER BY day