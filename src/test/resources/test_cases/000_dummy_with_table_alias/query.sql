SELECT "timestamp", ns_tenant_id, alert_type, SUM("count") AS total_type_alerts
FROM "redshift_poc_iceberg"."alert_v3" AS A
WHERE ns_tenant_id = 123 AND alert_type = 'XSS'
GROUP BY "timestamp", ns_tenant_id, alert_type
ORDER BY "timestamp" DESC