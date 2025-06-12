SELECT
    timestamp,
    ns_tenant_id,
    alert_type,
    action,
    SUM("count") AS count
FROM "redshift_poc_iceberg"."alert_v3"
WHERE ns_tenant_id = 2683
    AND timestamp >= TIMESTAMP '2025-01-01 15:30:00'
    AND timestamp < TIMESTAMP '2025-01-01 16:30:00'
GROUP BY timestamp, ns_tenant_id, alert_type, action
ORDER BY timestamp