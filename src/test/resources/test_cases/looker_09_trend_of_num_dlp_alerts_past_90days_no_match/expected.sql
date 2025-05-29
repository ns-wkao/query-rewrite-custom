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
              (A.TIMESTAMP) >= (TIMESTAMP '2025-01-03')
              AND (A.TIMESTAMP) < (TIMESTAMP '2025-01-17')
            )
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
          z___min_rank
      ) AS z___pivot_row_rank,
      RANK() OVER (
        PARTITION BY
          z__pivot_col_rank
        ORDER BY
          z___min_rank
      ) AS z__pivot_col_ordering,
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
              "alert_event.event_timestamp_date"
          ) AS z___min_rank
        FROM
          (
            SELECT
              *,
              RANK() OVER (
                ORDER BY
                  "alert_event.event_timestamp_date" DESC,
                  z__pivot_col_rank
              ) AS z___rank
            FROM
              (
                SELECT
                  *,
                  DENSE_RANK() OVER (
                    ORDER BY
                      CASE
                        WHEN "alert_event.access_method" IS NULL THEN 1
                        ELSE 0
                      END,
                      "alert_event.access_method"
                  ) AS z__pivot_col_rank
                FROM
                  (
                    SELECT
                      alert_event.access_method AS "alert_event.access_method",
                      (DATE_FORMAT(alert_event.TIMESTAMP, '%Y-%m-%d')) AS "alert_event.event_timestamp_date",
                      COALESCE(SUM(alert_event.count), 0) AS "alert_event.event_count"
                    FROM
                      alert_event
                    WHERE
                      (
                        (
                          (alert_event.TIMESTAMP) >= (TIMESTAMP '2025-01-03')
                          AND (alert_event.TIMESTAMP) < (TIMESTAMP '2025-01-17')
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
    z___pivot_row_rank <= 500
    OR z__pivot_col_ordering = 1
  )
ORDER BY
  z___pivot_row_rank