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
  CASE
    WHEN DATE_TRUNC(
      'month',
      DATE((DATE_FORMAT(alert_event.timestamp, '%Y-%m-%d')))
    ) = DATE_TRUNC(
      'month',
      CAST(
        (
          CASE
            WHEN NULL IS NULL
            OR DATE((DATE_FORMAT(CURRENT_TIMESTAMP, '%Y-%m-%d'))) <= DATE(NULL) THEN DATE((DATE_FORMAT(CURRENT_TIMESTAMP, '%Y-%m-%d')))
            ELSE DATE(NULL)
          END
        ) AS DATE
      )
    ) THEN 'Reference Period'
    WHEN DATE_TRUNC(
      'month',
      DATE((DATE_FORMAT(alert_event.timestamp, '%Y-%m-%d')))
    ) = DATE_TRUNC(
      'month',
      DATE_ADD(
        'month',
        -1,
        CAST(
          (
            CASE
              WHEN NULL IS NULL
              OR DATE((DATE_FORMAT(CURRENT_TIMESTAMP, '%Y-%m-%d'))) <= DATE(NULL) THEN DATE((DATE_FORMAT(CURRENT_TIMESTAMP, '%Y-%m-%d')))
              ELSE DATE(NULL)
            END
          ) AS DATE
        )
      )
    ) THEN 'Previous Period'
    ELSE NULL
  END AS "alert_event.current_vs_previous_period",
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
  AND (alert_event.alert_type) = 'policy'
  AND (alert_event.ns_tenant_id) = 2683
  AND (
    1 = 1
    AND 1 = 1
    AND 1 = 1
    AND (
      CASE
        WHEN DATE_TRUNC(
          'month',
          DATE((DATE_FORMAT(alert_event.timestamp, '%Y-%m-%d')))
        ) = DATE_TRUNC(
          'month',
          CAST(
            (
              CASE
                WHEN NULL IS NULL
                OR DATE((DATE_FORMAT(CURRENT_TIMESTAMP, '%Y-%m-%d'))) <= DATE(NULL) THEN DATE((DATE_FORMAT(CURRENT_TIMESTAMP, '%Y-%m-%d')))
                ELSE DATE(NULL)
              END
            ) AS DATE
          )
        ) THEN 'Reference Period'
        WHEN DATE_TRUNC(
          'month',
          DATE((DATE_FORMAT(alert_event.timestamp, '%Y-%m-%d')))
        ) = DATE_TRUNC(
          'month',
          DATE_ADD(
            'month',
            -1,
            CAST(
              (
                CASE
                  WHEN NULL IS NULL
                  OR DATE((DATE_FORMAT(CURRENT_TIMESTAMP, '%Y-%m-%d'))) <= DATE(NULL) THEN DATE((DATE_FORMAT(CURRENT_TIMESTAMP, '%Y-%m-%d')))
                  ELSE DATE(NULL)
                END
              ) AS DATE
            )
          )
        ) THEN 'Previous Period'
        ELSE NULL
      END
    ) IS NOT NULL
    AND 1 = 1
    AND 1 = 1
  )
GROUP BY
  1
ORDER BY
  1 DESC
LIMIT
  500