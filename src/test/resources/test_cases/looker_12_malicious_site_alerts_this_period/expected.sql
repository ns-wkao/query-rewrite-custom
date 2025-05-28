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
          a.*,
          SPLIT(a.organization_unit, '/') ou,
          CONCAT(a.app, a.instance_id) ns_app_instance
        FROM
          redshift_poc_iceberg.alert_event_daily_sum_detailed a
        WHERE
          (
            (a.ns_tenant_id = 2683)
            AND (
              (a.TIMESTAMP >= TIMESTAMP '2025-01-03')
              AND (a.TIMESTAMP < TIMESTAMP '2025-01-17')
            )
          )
      )
  )
SELECT
  (
    CASE
      WHEN (
        date_trunc(
          'month',
          date(date_format(alert_event.TIMESTAMP, '%Y-%m-%d'))
        ) = date_trunc(
          'month',
          CAST(
            (
              CASE
                WHEN (
                  (null IS NULL)
                  OR (
                    date(date_format(current_timestamp, '%Y-%m-%d')) <= date(null)
                  )
                ) THEN date(date_format(current_timestamp, '%Y-%m-%d'))
                ELSE date(null)
              END
            ) AS date
          )
        )
      ) THEN 'Reference Period'
      WHEN (
        date_trunc(
          'month',
          date(date_format(alert_event.TIMESTAMP, '%Y-%m-%d'))
        ) = date_trunc(
          'month',
          date_add(
            'month',
            -1,
            CAST(
              (
                CASE
                  WHEN (
                    (null IS NULL)
                    OR (
                      date(date_format(current_timestamp, '%Y-%m-%d')) <= date(null)
                    )
                  ) THEN date(date_format(current_timestamp, '%Y-%m-%d'))
                  ELSE date(null)
                END
              ) AS date
            )
          )
        )
      ) THEN 'Previous Period'
      ELSE null
    END
  ) "alert_event.current_vs_previous_period",
  COALESCE(sum(alert_event.count), 0) "alert_event.event_count"
FROM
  alert_event
WHERE
  (
    (
      (alert_event.TIMESTAMP >= TIMESTAMP '2025-01-03')
      AND (alert_event.TIMESTAMP < TIMESTAMP '2025-01-17')
    )
    AND (alert_event.alert_type = 'malsite')
    AND (alert_event.ns_tenant_id = 2683)
    AND (
      (1 = 1)
      AND (1 = 1)
      AND (1 = 1)
      AND (
        (
          CASE
            WHEN (
              DATE_TRUNC(
                'month',
                DATE(DATE_FORMAT(alert_event.TIMESTAMP, '%Y-%m-%d'))
              ) = DATE_TRUNC(
                'month',
                CAST(
                  (
                    CASE
                      WHEN (
                        (null IS NULL)
                        OR (
                          DATE(DATE_FORMAT(current_timestamp, '%Y-%m-%d')) <= DATE(null)
                        )
                      ) THEN DATE(DATE_FORMAT(current_timestamp, '%Y-%m-%d'))
                      ELSE DATE(null)
                    END
                  ) AS DATE
                )
              )
            ) THEN 'Reference Period'
            WHEN (
              DATE_TRUNC(
                'month',
                DATE(DATE_FORMAT(alert_event.TIMESTAMP, '%Y-%m-%d'))
              ) = DATE_TRUNC(
                'month',
                DATE_ADD(
                  'month',
                  -1,
                  CAST(
                    (
                      CASE
                        WHEN (
                          (null IS NULL)
                          OR (
                            DATE(DATE_FORMAT(current_timestamp, '%Y-%m-%d')) <= DATE(null)
                          )
                        ) THEN DATE(DATE_FORMAT(current_timestamp, '%Y-%m-%d'))
                        ELSE DATE(null)
                      END
                    ) AS DATE
                  )
                )
              )
            ) THEN 'Previous Period'
            ELSE null
          END
        ) IS NOT NULL
      )
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