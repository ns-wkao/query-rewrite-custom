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
          (A.TIMESTAMP) >= (TIMESTAMP '2025-01-03')
          AND (A.TIMESTAMP) < (TIMESTAMP '2025-01-17')
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
  )
SELECT
  COUNT(
    DISTINCT (
      coalesce(app_info.app_current_name, page_event.app)
    )
  ) AS "page_event.distinct_app_count"
FROM
  page_event
  LEFT JOIN app_info ON page_event.app = app_info.appinfo_app
WHERE
  (
    (
      (page_event.TIMESTAMP) >= (TIMESTAMP '2025-01-03')
      AND (page_event.TIMESTAMP) < (TIMESTAMP '2025-01-17')
    )
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
LIMIT
  500