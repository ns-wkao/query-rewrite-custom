WITH
  alert_event AS (
    SELECT
      *,
      action derived_action,
      policy derived_policy,
      transaction_id derived_transaction_id
    FROM
      (
        SELECT
          A.*,
          SPLIT(A.organization_unit, '/') OU,
          CONCAT(A.app, A.instance_id) ns_app_instance
        FROM
          redshift_poc_iceberg.alert_event_daily_sum A
        WHERE
          (
            (A.ns_tenant_id = 2683)
            AND (
              (A.TIMESTAMP >= TIMESTAMP '2025-01-03')
              AND (A.TIMESTAMP < TIMESTAMP '2025-01-17')
            )
          )
      )
  )
SELECT
  COALESCE(SUM(alert_event.count), 0) "alert_event.event_count"
FROM
  alert_event
WHERE
  (
    (
      (alert_event.TIMESTAMP >= TIMESTAMP '2025-01-03')
      AND (alert_event.TIMESTAMP < TIMESTAMP '2025-01-17')
    )
    AND (
      alert_event.alert_type IN (
        'Compromised Credential',
        'DLP',
        'Malware',
        'Security Assessment',
        'anomaly',
        'malsite',
        'policy'
      )
    )
    AND (alert_event.ns_tenant_id = 2683)
    AND (
      (1 = 1)
      AND (1 = 1)
      AND (1 = 1)
      AND (1 = 1)
      AND (1 = 1)
      AND (1 = 1)
    )
  )
LIMIT
  500