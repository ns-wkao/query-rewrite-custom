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
                    -89,
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
                    90,
                    DATE_ADD(
                      'day',
                      -89,
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
  (
    DATE_FORMAT(
      DATE_TRUNC(
        'DAY',
        DATE_ADD(
          'day',
          (
            0 - MOD(
              (DAY_OF_WEEK(alert_event.TIMESTAMP) % 7) - 1 + 7,
              7
            )
          ),
          alert_event.TIMESTAMP
        )
      ),
      '%Y-%m-%d'
    )
  ) AS "alert_event.event_timestamp_week",
  COALESCE(SUM(alert_event.count), 0) AS "alert_event.event_count",
  COUNT(DISTINCT alert_event.derived_policy) AS "policies"
FROM
  alert_event
WHERE
  (
    (
      (alert_event.TIMESTAMP) >= (
        (
          DATE_ADD(
            'day',
            -89,
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
            90,
            DATE_ADD(
              'day',
              -89,
              CAST(
                CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP
              )
            )
          )
        )
      )
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
  1 DESC
LIMIT
  500