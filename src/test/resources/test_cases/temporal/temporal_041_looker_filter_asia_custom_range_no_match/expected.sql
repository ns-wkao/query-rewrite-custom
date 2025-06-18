WITH
  alert_event AS (
    SELECT
      *,
      action AS derived_action,
      policy AS derived_policy,
      transaction_id AS derived_transaction_id
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
              (A.TIMESTAMP) >= (
                (
                  DATE_ADD(
                    'day',
                    -6,
                    CAST(
                      CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP
                    )
                  )
                )
              )
              AND (A.TIMESTAMP) < (
                (
                  DATE_ADD(
                    'day',
                    7,
                    DATE_ADD(
                      'day',
                      -6,
                      CAST(
                        CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP
                      )
                    )
                  )
                )
              )
            )
          )
      )
  )
SELECT
  alert_event.alert_type AS "alert_event.alert_type",
  COUNT(*) AS "alert_event.event_count"
FROM
  alert_event
WHERE
  (
    (
      (alert_event.TIMESTAMP) >= (
        (
          DATE_ADD(
            'day',
            -6,
            CAST(
              CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP
            )
          )
        )
      )
      AND (alert_event.TIMESTAMP) < (
        (
          DATE_ADD(
            'day',
            7,
            DATE_ADD(
              'day',
              -6,
              CAST(
                CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP
              )
            )
          )
        )
      )
    )
  )
  AND (
    (
      (CAST(alert_event.TIMESTAMP AS TIMESTAMP)) >= (TIMESTAMP '2025-06-05 03:30')
      AND (CAST(alert_event.TIMESTAMP AS TIMESTAMP)) < (TIMESTAMP '2025-06-10 16:30')
    )
  )
  AND (alert_event.ns_tenant_id) = 2683
  AND (
    1 = 1
    AND 1 = 1
    AND 1 = 1
    AND 1 = 1
    AND 1 = 1
    AND 1 = 1
  )
GROUP BY
  1
ORDER BY
  2 DESC
LIMIT
  500