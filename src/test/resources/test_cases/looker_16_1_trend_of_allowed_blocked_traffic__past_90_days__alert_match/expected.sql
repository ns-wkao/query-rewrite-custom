WITH
  alert_event AS (
    SELECT
      *,
      (
        CASE
          WHEN (alert_type = 'Malware') THEN policy_alerts.policy_action
          ELSE alerts.action
        END
      ) derived_action,
      (
        CASE
          WHEN (alert_type = 'Malware') THEN policy_alerts.policy_name
          ELSE alerts.policy
        END
      ) derived_policy,
      alerts.transaction_id derived_transaction_id
    FROM
      (
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
        ) alerts
        LEFT JOIN (
          SELECT
            alert_type policy_alert_type,
            transaction_id,
            action policy_action,
            policy policy_name
          FROM
            redshift_poc_iceberg.alert_event_daily_sum_detailed
          WHERE
            (
              (transaction_id IS NOT NULL)
              AND (ns_tenant_id = 2683)
              AND (
                (
                  (alert_type = 'policy')
                  AND (malware_name IS NOT NULL)
                )
                OR (
                  (alert_type IN ('ips', 'ctep'))
                  AND (action IN ('block', 'blocked'))
                )
              )
              AND (
                (
                  TIMESTAMP >= DATE_ADD(
                    'day',
                    -89,
                    CAST(
                      CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP
                    )
                  )
                )
                AND (
                  TIMESTAMP < DATE_ADD(
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
        ) policy_alerts ON (
          policy_alerts.transaction_id = alerts.transaction_id
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
  COALESCE(
    SUM(
      COALESCE(
        TRY((alert_event.numbytes / DECIMAL '1073741824.0')),
        0
      )
    ),
    0
  ) "alert_event.sum_total_bytes_gb",
  COALESCE(
    SUM(
      COALESCE(
        TRY((alert_event.file_size / DECIMAL '1073741824.0')),
        0
      )
    ),
    0
  ) "alert_event.sum_file_size_gb"
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
    AND (alert_event.derived_action = 'block')
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