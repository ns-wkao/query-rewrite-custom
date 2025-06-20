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
  ),
  app_category AS (
    SELECT
      *
    FROM
      "redshift_poc_iceberg"."appcategory"
    WHERE
      ns_tenant_id IN (2683, -1)
  )
SELECT
  *
FROM
  (
    SELECT
      *,
      DENSE_RANK() OVER (
        ORDER BY
          z___min_rank
      ) as z___pivot_row_rank,
      RANK() OVER (
        PARTITION BY
          z__pivot_col_rank
        ORDER BY
          z___min_rank
      ) as z__pivot_col_ordering,
      CASE
        WHEN z___min_rank = z___rank THEN 1
        ELSE 0
      END AS z__is_highest_ranked_cell
    FROM
      (
        SELECT
          *,
          MIN(z___rank) OVER (
            PARTITION BY
              "alert_event.appcategory"
          ) as z___min_rank
        FROM
          (
            SELECT
              *,
              RANK() OVER (
                ORDER BY
                  CASE
                    WHEN z__pivot_col_rank = 6 THEN (
                      CASE
                        WHEN "alert_event.event_count" IS NOT NULL THEN 0
                        ELSE 1
                      END
                    )
                    ELSE 2
                  END,
                  CASE
                    WHEN z__pivot_col_rank = 6 THEN "alert_event.event_count"
                    ELSE NULL
                  END DESC,
                  "alert_event.event_count" DESC,
                  z__pivot_col_rank,
                  "alert_event.appcategory"
              ) AS z___rank
            FROM
              (
                SELECT
                  *,
                  DENSE_RANK() OVER (
                    ORDER BY
                      CASE
                        WHEN "alert_event.dlp_rule_severity" IS NULL THEN 1
                        ELSE 0
                      END,
                      "alert_event.dlp_rule_severity"
                  ) AS z__pivot_col_rank
                FROM
                  (
                    SELECT
                      alert_event.dlp_rule_severity AS "alert_event.dlp_rule_severity",
                      coalesce(
                        app_category.app_category_current_name,
                        alert_event.appcategory
                      ) AS "alert_event.appcategory",
                      COALESCE(SUM(alert_event.count), 0) AS "alert_event.event_count"
                    FROM
                      alert_event
                      LEFT JOIN app_category ON alert_event.ns_category_id = app_category.category_id
                    WHERE
                      (
                        (
                          (alert_event.timestamp) >= (TIMESTAMP '2025-01-03')
                          AND (alert_event.timestamp) < (TIMESTAMP '2025-01-17')
                        )
                      )
                      AND (alert_event.alert_type) = 'DLP'
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
                      1,
                      2
                  ) ww
              ) bb
            WHERE
              z__pivot_col_rank <= 16384
          ) aa
      ) xx
  ) zz
WHERE
  (
    z__pivot_col_rank <= 50
    OR z__is_highest_ranked_cell = 1
  )
  AND (
    z___pivot_row_rank <= 5000
    OR z__pivot_col_ordering = 1
  )
ORDER BY
  z___pivot_row_rank