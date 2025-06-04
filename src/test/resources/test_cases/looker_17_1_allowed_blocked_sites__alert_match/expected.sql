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
                    -6,
                    CAST(
                      CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP
                    )
                  )
                )
                AND (
                  A.TIMESTAMP < DATE_ADD(
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
                    -6,
                    CAST(
                      CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP
                    )
                  )
                )
                AND (
                  TIMESTAMP < DATE_ADD(
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
        ) policy_alerts ON (
          policy_alerts.transaction_id = alerts.transaction_id
        )
      )
  ),
  app_category AS (
    SELECT
      *
    FROM
      "redshift_poc_iceberg"."appcategory"
    WHERE
      (ns_tenant_id IN (2683, -1))
  )
SELECT
  COALESCE(
    app_category.app_category_current_name,
    alert_event.appcategory
  ) "alert_event.appcategory",
  COALESCE(SUM(alert_event.count), 0) "alert_event.event_count",
  COUNT(DISTINCT alert_event.site) "alert_event.distinct_site_count"
FROM
  (
    alert_event
    LEFT JOIN app_category ON (
      alert_event.ns_category_id = app_category.category_id
    )
  )
WHERE
  (
    (
      (
        alert_event.TIMESTAMP >= DATE_ADD(
          'day',
          -6,
          CAST(
            CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP
          )
        )
      )
      AND (
        alert_event.TIMESTAMP < DATE_ADD(
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
    AND (
      COALESCE(
        app_category.app_category_current_name,
        alert_event.appcategory
      ) IS NOT NULL
    )
  )
GROUP BY
  1
ORDER BY
  3 DESC
LIMIT
  15