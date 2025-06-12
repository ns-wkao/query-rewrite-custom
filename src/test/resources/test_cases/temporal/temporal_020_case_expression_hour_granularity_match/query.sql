-- CASE with hour/day granularities - should match HOUR MV
SELECT 
    CASE 
        WHEN action = 'block' THEN date_trunc('hour', timestamp)
        ELSE date_trunc('day', timestamp)  
    END AS time_bucket,
    ns_tenant_id,
    alert_type,
    SUM("count") AS total_count
FROM "redshift_poc_iceberg"."alert_v3"
WHERE ns_tenant_id = 2683
GROUP BY 1, ns_tenant_id, alert_type
ORDER BY time_bucket