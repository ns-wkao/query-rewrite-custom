SELECT
    alerts.transaction_id,
    alerts.alert_type,
    alerts.action,
    policy_alerts.policy_name,
    policy_alerts.malware_name
FROM
    (
        SELECT transaction_id, alert_type, action, policy, ns_tenant_id
        FROM "redshift_poc_iceberg"."alert_v3"
        WHERE ns_tenant_id = 1234 AND alert_type = 'Malware'
    ) AS alerts
LEFT OUTER JOIN
    (
        SELECT transaction_id, policy AS policy_name, action AS policy_action, malware_name
        FROM "redshift_poc_iceberg"."alert_v3"
        WHERE ns_tenant_id = 1234 AND alert_type = 'policy' AND malware_name IS NOT NULL
    ) AS policy_alerts
ON alerts.transaction_id = policy_alerts.transaction_id