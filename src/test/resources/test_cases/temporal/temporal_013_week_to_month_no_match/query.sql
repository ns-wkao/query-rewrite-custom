SELECT 
    date_trunc('month', timestamp) as month,
    ns_id,
    organization_unit,
    appcategory,
    SUM("count") as total_count
FROM "redshift_poc_iceberg"."alert_v3"
GROUP BY 
    date_trunc('month', timestamp),
    ns_id,
    organization_unit,
    appcategory
ORDER BY month