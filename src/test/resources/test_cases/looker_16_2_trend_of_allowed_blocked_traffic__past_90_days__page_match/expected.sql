WITH
  page_event AS (
    SELECT
      A.*,
      SPLIT(A.organization_unit, '/') OU
    FROM
      redshift_poc_iceberg.page_event_30min_sum A
    WHERE
      (
        (A.ns_tenant_id = 2683)
        AND (
          (
            A.TIMESTAMP >= DATE_ADD(
              'day',
              -89,
              CAST(
                CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP
              )
            )
          )
          AND (
            A.TIMESTAMP < DATE_ADD(
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
SELECT
  DATE_FORMAT(
    DATE_TRUNC(
      'DAY',
      DATE_ADD(
        'day',
        (
          0 - MOD(
            (((DAY_OF_WEEK(page_event.TIMESTAMP) % 7) - 1) + 7),
            7
          )
        ),
        page_event.TIMESTAMP
      )
    ),
    '%Y-%m-%d'
  ) "page_event.event_timestamp_week",
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
  page_event
WHERE
  (
    (
      (
        page_event.TIMESTAMP >= DATE_ADD(
          'day',
          -89,
          CAST(
            CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP
          )
        )
      )
      AND (
        page_event.TIMESTAMP < DATE_ADD(
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
  1 DESC
LIMIT
  500