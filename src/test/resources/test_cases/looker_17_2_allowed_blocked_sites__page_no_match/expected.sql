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
          (A.TIMESTAMP) >= (
            (
              DATE_ADD(
                'day',
                -6,
                CAST(
                  CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP
                )
              )
            )
          )
          AND (A.TIMESTAMP) < (
            (
              DATE_ADD(
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
  coalesce(
    app_category.app_category_current_name,
    page_event.appcategory
  ) AS "page_event.appcategory",
  COALESCE(SUM(page_event.count), 0) AS "page_event.event_count",
  COUNT(DISTINCT page_event.site) AS "page_event.distinct_site_count"
FROM
  page_event
  LEFT JOIN app_category ON page_event.ns_category_id = app_category.category_id
WHERE
  (
    (
      (page_event.TIMESTAMP) >= (
        (
          DATE_ADD(
            'day',
            -6,
            CAST(
              CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP
            )
          )
        )
      )
      AND (page_event.TIMESTAMP) < (
        (
          DATE_ADD(
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
  )
  AND (page_event.ns_tenant_id) = 2683
GROUP BY
  1
ORDER BY
  2 DESC
LIMIT
  500