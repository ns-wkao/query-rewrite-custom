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
          (A.timestamp) >= (
            (
              DATE_ADD(
                'day',
                -89,
                CAST(
                  CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP
                )
              )
            )
          )
          AND (A.timestamp) < (
            (
              DATE_ADD(
                'day',
                90,
                DATE_ADD(
                  'day',
                  -89,
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
  (
    DATE_FORMAT(
      DATE_TRUNC(
        'DAY',
        DATE_ADD(
          'day',
          (
            0 - MOD(
              (DAY_OF_WEEK(page_event.timestamp) % 7) - 1 + 7,
              7
            )
          ),
          page_event.timestamp
        )
      ),
      '%Y-%m-%d'
    )
  ) AS "page_event.event_timestamp_week",
  COUNT(DISTINCT page_event.ur_normalized) AS "page_event.distinct_user_count",
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
      (page_event.timestamp) >= (
        (
          DATE_ADD(
            'day',
            -89,
            CAST(
              CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP
            )
          )
        )
      )
      AND (page_event.timestamp) < (
        (
          DATE_ADD(
            'day',
            90,
            DATE_ADD(
              'day',
              -89,
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
  AND (
    1 = 1
    AND 1 = 1
    AND 1 = 1
    AND 1 = 1
    AND 1 = 1
    AND 1 = 1
  )
GROUP BY
  1
ORDER BY
  1 DESC
LIMIT
  500