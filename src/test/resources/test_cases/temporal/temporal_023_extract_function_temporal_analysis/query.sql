-- extract() function - should detect hour granularity requirement
SELECT 
    extract(hour from timestamp) AS hour_of_day,
    ns_tenant_id,
    alert_type,
    SUM("count") AS total_count
FROM "redshift_poc_iceberg"."alert_v3"
WHERE ns_tenant_id = 2683 
    AND extract(hour from timestamp) = 15
GROUP BY 1, ns_tenant_id, alert_type
ORDER BY hour_of_day