WITH
  page_event AS (
    SELECT
      A.*,
      SPLIT(A.organization_unit, '/') OU
    FROM
      redshift_poc_iceberg.page_event_daily_sum A
    WHERE
      (
        (A.ns_tenant_id = 2683)
        AND (
          (A.TIMESTAMP >= TIMESTAMP '2025-01-03')
          AND (A.TIMESTAMP < TIMESTAMP '2025-01-17')
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
          ) appinfo_row_rank
        FROM
          "redshift_poc_iceberg"."appinfo"
        WHERE
          (ns_tenant_id IN (2683, -1))
      )
    WHERE
      (appinfo_row_rank = 1)
  )
SELECT
  COALESCE(app_info.app_current_name, page_event.app) "page_event.app",
  COALESCE(
    SUM(
      COALESCE(
        TRY(
          (page_event.client_bytes / DECIMAL '1073741824.0')
        ),
        0
      )
    ),
    0
  ) "page_event.sum_bytes_uploaded_gb",
  COALESCE(
    SUM(
      COALESCE(
        TRY(
          (page_event.server_bytes / DECIMAL '1073741824.0')
        ),
        0
      )
    ),
    0
  ) "page_event.sum_bytes_downloaded_gb",
  COALESCE(
    SUM(
      COALESCE(
        TRY((page_event.numbytes / DECIMAL '1073741824.0')),
        0
      )
    ),
    0
  ) "page_event.sum_total_bytes_gb"
FROM
  (
    page_event
    LEFT JOIN app_info ON (page_event.app = app_info.appinfo_app)
  )
WHERE
  (
    (
      (page_event.TIMESTAMP >= TIMESTAMP '2025-01-03')
      AND (page_event.TIMESTAMP < TIMESTAMP '2025-01-17')
    )
    AND (app_info.appinfo_ccl IN ('low', 'medium', 'poor'))
    AND (
      (NOT app_info.sanctioned)
      OR (app_info.sanctioned IS NULL)
    )
    AND (page_event.ns_tenant_id = 2683)
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
  4 DESC
LIMIT
  5