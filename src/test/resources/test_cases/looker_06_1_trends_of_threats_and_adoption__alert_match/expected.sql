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
          redshift_poc_iceberg.alert_event_daily_sum_detailed A
        WHERE
          (
            (A.ns_tenant_id = 2683)
            AND (
              (
                A.TIMESTAMP >= DATE_ADD(
                  'day',
                  -89,
                  CAST(
                    CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP
                  )
                )
              )
              AND (
                A.TIMESTAMP < DATE_ADD(
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
SELECT
  DATE_FORMAT(
    DATE_TRUNC(
      'DAY',
      DATE_ADD(
        'day',
        (
          0 - MOD(
            (
              ((DAY_OF_WEEK(alert_event.TIMESTAMP) % 7) - 1) + 7
            ),
            7
          )
        ),
        alert_event.TIMESTAMP
      )
    ),
    '%Y-%m-%d'
  ) "alert_event.event_timestamp_week",
  COALESCE(SUM(alert_event.count), 0) "alert_event.event_count",
  COUNT(DISTINCT alert_event.derived_policy) "policies"
FROM
  alert_event
WHERE
  (
    (
      (
        alert_event.TIMESTAMP >= DATE_ADD(
          'day',
          -89,
          CAST(
            CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP
          )
        )
      )
      AND (
        alert_event.TIMESTAMP < DATE_ADD(
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
GROUP BY
  1
ORDER BY
  1 DESC
LIMIT
  500