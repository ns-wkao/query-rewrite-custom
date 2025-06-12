WITH
  page_event AS (
    SELECT
      A.*,
      SPLIT(A.organization_unit, '/') AS OU
    FROM
      "redshift_poc_iceberg"."page_v3" AS A
    WHERE
      A.ns_tenant_id = 2683
      AND (
        (
          (A.timestamp) >= (TIMESTAMP '2025-01-03')
          AND (A.timestamp) < (TIMESTAMP '2025-01-17')
        )
      )
  ),
  app_info AS (
    SELECT
      *
    FROM
      (
        SELECT
          *,
          ROW_NUMBER() OVER (
            PARTITION BY
              app_id
            ORDER BY
              ns_tenant_id DESC
          ) AS appinfo_row_rank
        FROM
          "redshift_poc_iceberg"."appinfo"
        WHERE
          ns_tenant_id IN (2683, -1)
      )
    WHERE
      appinfo_row_rank = 1
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
              "page_event.appcategory"
          ) as z___min_rank
        FROM
          (
            SELECT
              *,
              RANK() OVER (
                ORDER BY
                  CASE
                    WHEN z__pivot_col_rank = 4 THEN (
                      CASE
                        WHEN "page_event.sum_total_bytes" IS NOT NULL THEN 0
                        ELSE 1
                      END
                    )
                    ELSE 2
                  END,
                  CASE
                    WHEN z__pivot_col_rank = 4 THEN "page_event.sum_total_bytes"
                    ELSE NULL
                  END DESC,
                  "page_event.sum_total_bytes" DESC,
                  z__pivot_col_rank,
                  "page_event.appcategory"
              ) AS z___rank
            FROM
              (
                SELECT
                  *,
                  DENSE_RANK() OVER (
                    ORDER BY
                      CASE
                        WHEN "app_info.sorted_ccl" IS NULL THEN 1
                        ELSE 0
                      END,
                      "app_info.sorted_ccl",
                      CASE
                        WHEN "app_info.ccl" IS NULL THEN 1
                        ELSE 0
                      END,
                      "app_info.ccl"
                  ) AS z__pivot_col_rank
                FROM
                  (
                    SELECT
                      app_info.appinfo_ccl AS "app_info.ccl",
                      coalesce(
                        if(app_info.appinfo_ccl = 'excellent', 'a', null),
                        if(app_info.appinfo_ccl = 'high', 'b', null),
                        if(app_info.appinfo_ccl = 'medium', 'c', null),
                        if(app_info.appinfo_ccl = 'low', 'd', null),
                        if(app_info.appinfo_ccl = 'poor', 'e', null),
                        'unknown'
                      ) AS "app_info.sorted_ccl",
                      coalesce(
                        app_category.app_category_current_name,
                        page_event.appcategory
                      ) AS "page_event.appcategory",
                      COALESCE(SUM(page_event.numbytes), 0) AS "page_event.sum_total_bytes"
                    FROM
                      page_event
                      LEFT JOIN app_info ON page_event.app = app_info.appinfo_app
                      LEFT JOIN app_category ON page_event.ns_category_id = app_category.category_id
                    WHERE
                      (
                        (
                          (page_event.timestamp) >= (TIMESTAMP '2025-01-03')
                          AND (page_event.timestamp) < (TIMESTAMP '2025-01-17')
                        )
                      )
                      AND (app_info.appinfo_ccl) IN ('low', 'medium', 'poor')
                      AND (
                        NOT (app_info.sanctioned)
                        OR (app_info.sanctioned) IS NULL
                      )
                      AND (page_event.ns_tenant_id) = 2683
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
                      2,
                      3
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