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
                A.TIMESTAMP >= CAST(
                  CONCAT(
                    FORMAT_DATETIME(
                      DATE_ADD(
                        'day',
                        -6,
                        CAST(
                          CAST(
                            DATE_TRUNC('DAY', NOW() AT TIME ZONE 'Asia/Taipei') AS DATE
                          ) AS TIMESTAMP
                        )
                      ),
                      'yyyy-MM-dd HH:mm:ss.SSS'
                    ),
                    ' Asia/Taipei'
                  ) AS TIMESTAMP
                )
              )
              AND (
                A.TIMESTAMP < CAST(
                  CONCAT(
                    FORMAT_DATETIME(
                      DATE_ADD(
                        'day',
                        7,
                        DATE_ADD(
                          'day',
                          -6,
                          CAST(
                            CAST(
                              DATE_TRUNC('DAY', NOW() AT TIME ZONE 'Asia/Taipei') AS DATE
                            ) AS TIMESTAMP
                          )
                        )
                      ),
                      'yyyy-MM-dd HH:mm:ss.SSS'
                    ),
                    ' Asia/Taipei'
                  ) AS TIMESTAMP
                )
              )
            )
          )
      )
  )
SELECT
  alert_event.alert_type "alert_event.alert_type",
  COUNT(*) "alert_event.event_count"
FROM
  alert_event
WHERE
  (
    (
      (
        alert_event.TIMESTAMP >= CAST(
          CONCAT(
            FORMAT_DATETIME(
              DATE_ADD(
                'day',
                -6,
                CAST(
                  CAST(
                    DATE_TRUNC('DAY', NOW() AT TIME ZONE 'Asia/Taipei') AS DATE
                  ) AS TIMESTAMP
                )
              ),
              'yyyy-MM-dd HH:mm:ss.SSS'
            ),
            ' Asia/Taipei'
          ) AS TIMESTAMP
        )
      )
      AND (
        alert_event.TIMESTAMP < CAST(
          CONCAT(
            FORMAT_DATETIME(
              DATE_ADD(
                'day',
                7,
                DATE_ADD(
                  'day',
                  -6,
                  CAST(
                    CAST(
                      DATE_TRUNC('DAY', NOW() AT TIME ZONE 'Asia/Taipei') AS DATE
                    ) AS TIMESTAMP
                  )
                )
              ),
              'yyyy-MM-dd HH:mm:ss.SSS'
            ),
            ' Asia/Taipei'
          ) AS TIMESTAMP
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
  2 DESC
LIMIT
  500