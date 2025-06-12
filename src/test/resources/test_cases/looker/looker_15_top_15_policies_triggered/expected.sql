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
                (A.TIMESTAMP >= TIMESTAMP '2025-01-03')
                AND (A.TIMESTAMP < TIMESTAMP '2025-01-17')
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
                (TIMESTAMP >= TIMESTAMP '2025-01-03')
                AND (TIMESTAMP < TIMESTAMP '2025-01-17')
              )
            )
        ) policy_alerts ON (
          policy_alerts.transaction_id = alerts.transaction_id
        )
      )
  )
SELECT
  *
FROM
  (
    SELECT
      *,
      DENSE_RANK() OVER (
        ORDER BY
          z___min_rank ASC
      ) z___pivot_row_rank,
      RANK() OVER (
        PARTITION BY
          z__pivot_col_rank
        ORDER BY
          z___min_rank ASC
      ) z__pivot_col_ordering,
      (
        CASE
          WHEN (z___min_rank = z___rank) THEN 1
          ELSE 0
        END
      ) z__is_highest_ranked_cell
    FROM
      (
        SELECT
          *,
          MIN(z___rank) OVER (
            PARTITION BY
              "alert_event.policy"
          ) z___min_rank
        FROM
          (
            SELECT
              *,
              RANK() OVER (
                ORDER BY
                  (
                    CASE
                      WHEN (z__pivot_col_rank = 1) THEN (
                        CASE
                          WHEN ("alert_event.event_count" IS NOT NULL) THEN 0
                          ELSE 1
                        END
                      )
                      ELSE 2
                    END
                  ) ASC,
                  (
                    CASE
                      WHEN (z__pivot_col_rank = 1) THEN "alert_event.event_count"
                      ELSE null
                    END
                  ) DESC,
                  "alert_event.event_count" DESC,
                  z__pivot_col_rank ASC,
                  "alert_event.policy" ASC
              ) z___rank
            FROM
              (
                SELECT
                  *,
                  DENSE_RANK() OVER (
                    ORDER BY
                      (
                        CASE
                          WHEN ("alert_event.action" IS NULL) THEN 1
                          ELSE 0
                        END
                      ) ASC,
                      "alert_event.action" ASC
                  ) z__pivot_col_rank
                FROM
                  (
                    SELECT
                      alert_event.derived_action "alert_event.action",
                      alert_event.derived_policy "alert_event.policy",
                      COALESCE(SUM(alert_event.count), 0) "alert_event.event_count"
                    FROM
                      alert_event
                    WHERE
                      (
                        (
                          (alert_event.TIMESTAMP >= TIMESTAMP '2025-01-03')
                          AND (alert_event.TIMESTAMP < TIMESTAMP '2025-01-17')
                        )
                        AND (alert_event.alert_type = 'policy')
                        AND (alert_event.ns_tenant_id = 2683)
                        AND (
                          (1 = 1)
                          AND (1 = 1)
                          AND (1 = 1)
                          AND (1 = 1)
                          AND (1 = 1)
                          AND (1 = 1)
                          AND (1 = 1)
                        )
                        AND (alert_event.derived_policy IS NOT NULL)
                      )
                    GROUP BY
                      1,
                      2
                  ) ww
              ) bb
            WHERE
              (z__pivot_col_rank <= 16384)
          ) aa
      ) xx
  ) zz
WHERE
  (
    (
      (z__pivot_col_rank <= 50)
      OR (z__is_highest_ranked_cell = 1)
    )
    AND (
      (z___pivot_row_rank <= 5000)
      OR (z__pivot_col_ordering = 1)
    )
  )
ORDER BY
  z___pivot_row_rank ASC