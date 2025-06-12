-- date_format() wrapping date_trunc() - should detect underlying granularity
SELECT 
    date_format(date_trunc('hour', timestamp), '%Y-%m-%d %H:00:00') AS formatted_hour,
    ns_tenant_id,
    SUM("count") AS total_count
FROM "redshift_poc_iceberg"."alert_v3"
WHERE action = 'alert'
GROUP BY 1, ns_tenant_id
ORDER BY formatted_hour