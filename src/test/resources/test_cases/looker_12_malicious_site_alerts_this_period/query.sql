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
          a.*,
          Split(a.organization_unit, '/') AS ou,
          Concat(a.app, a.instance_id) AS ns_app_instance
        FROM
          "redshift_poc_iceberg"."alert_v3" AS a
        WHERE
          a.ns_tenant_id = 2683
          AND (
            (
              (a.timestamp) >= (timestamp '2025-01-03')
              AND (a.timestamp) < (timestamp '2025-01-17')
            )
          )
      )
  )
SELECT
  CASE
    WHEN date_trunc(
      'month',
      date((date_format(alert_event.timestamp, '%Y-%m-%d')))
    ) = date_trunc(
      'month',
      cast(
        (
          CASE
            WHEN NULL IS NULL
            OR date((date_format(CURRENT_TIMESTAMP, '%Y-%m-%d'))) <= date(NULL) THEN date((date_format(CURRENT_TIMESTAMP, '%Y-%m-%d')))
            ELSE date(NULL)
          END
        ) AS date
      )
    ) THEN 'Reference Period'
    WHEN date_trunc(
      'month',
      date((date_format(alert_event.timestamp, '%Y-%m-%d')))
    ) = date_trunc(
      'month',
      date_add(
        'month',
        -1,
        cast(
          (
            CASE
              WHEN NULL IS NULL
              OR date((date_format(CURRENT_TIMESTAMP, '%Y-%m-%d'))) <= date(NULL) THEN date((date_format(CURRENT_TIMESTAMP, '%Y-%m-%d')))
              ELSE date(NULL)
            END
          ) AS date
        )
      )
    ) THEN 'Previous Period'
    ELSE NULL
  END AS "alert_event.current_vs_previous_period",
  COALESCE(sum(alert_event.count), 0) AS "alert_event.event_count"
FROM
  alert_event
WHERE
  (
    (
      (alert_event.timestamp) >= (timestamp '2025-01-03')
      AND (alert_event.timestamp) < (timestamp '2025-01-17')
    )
  )
  AND (alert_event.alert_type) = 'malsite'
  AND (alert_event.ns_tenant_id) = 2683
  AND (
    1 = 1
    AND 1 = 1
    AND 1 = 1
    AND (
      CASE
        WHEN DATE_TRUNC(
          'month',
          DATE((DATE_FORMAT(alert_event.TIMESTAMP, '%Y-%m-%d')))
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
          DATE((DATE_FORMAT(alert_event.TIMESTAMP, '%Y-%m-%d')))
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