SELECT ns_tenant_id, alert_type, SUM("count")
FROM "redshift_poc_iceberg"."alert_v3"
GROUP BY ns_tenant_id, alert_type