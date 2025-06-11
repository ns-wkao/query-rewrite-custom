-- Test: MV with daily granularity can satisfy query with no temporal grouping
-- Expected: MATCH - MV can satisfy non-temporal queries
SELECT 
    ns_tenant_id,
    alert_type,
    SUM("count") as total_count
FROM "redshift_poc_iceberg"."alert_v3"
WHERE action = 'alert'
GROUP BY 
    ns_tenant_id, 
    alert_type
ORDER BY ns_tenant_id, alert_type