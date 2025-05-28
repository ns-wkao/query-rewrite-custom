SELECT ns_tenant_id, alert_type, SUM("count")
FROM poc_mvs.mv_simple_agg
GROUP BY ns_tenant_id, alert_type