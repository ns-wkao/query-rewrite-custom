SELECT 
    CASE 
        WHEN alert_type = 'DLP' THEN date_trunc('day', timestamp)
        WHEN alert_type = 'MALWARE' THEN date_trunc('hour', timestamp) 
        ELSE date_trunc('minute', timestamp)
    END AS time_bucket,
    ns_tenant_id,
    SUM("count") AS total_count
FROM "redshift_poc_iceberg"."alert_v3"
WHERE ns_tenant_id = 2683
GROUP BY 1, ns_tenant_id
ORDER BY time_bucket