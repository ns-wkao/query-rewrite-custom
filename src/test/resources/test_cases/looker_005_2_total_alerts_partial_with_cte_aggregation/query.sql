WITH
  alert_event AS (
    SELECT
      *,
      action as derived_action,
      policy as derived_policy,
      transaction_id as derived_transaction_id
    FROM
      (
        SELECT
          A.*,
          SPLIT(A.organization_unit, '/') AS OU,
          CONCAT(A.app, A.instance_id) AS ns_app_instance
        FROM
          "redshift_poc_iceberg"."alert_v3" AS A
        WHERE
          A.ns_tenant_id = 2683
          AND (
            (
              (A.timestamp) >= (TIMESTAMP '2025-01-03')
              AND (A.timestamp) < (TIMESTAMP '2025-01-17')
            )
          )
      )
  )
SELECT
  alert_event.alert_type AS "alert_event.alert_type",
  COALESCE(SUM(alert_event.count), 0) AS "alert_event.event_count"
FROM
  alert_event
WHERE
  (
    (
      (alert_event.timestamp) >= (TIMESTAMP '2025-01-03')
      AND (alert_event.timestamp) < (TIMESTAMP '2025-01-17')
    )
  )
  AND (alert_event.alert_type) IN (
    'Compromised Credential',
    'DLP',
    'Malware',
    'Security Assessment',
    'anomaly',
    'malsite',
    'policy'
  )
  AND (alert_event.ns_tenant_id) = 2683
GROUP BY
  alert_event.alert_type
ORDER BY
  COALESCE(SUM(alert_event.count), 0) DESC
LIMIT
  500