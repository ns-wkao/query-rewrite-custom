WITH
  page_event AS (
    SELECT
      A.*,
      SPLIT(A.organization_unit, '/') AS OU
    FROM
      "redshift_poc_iceberg"."page_v3" AS A
    WHERE
      A.ns_tenant_id = 2683
      AND
      (
        (
          (A.timestamp) >= (TIMESTAMP '2025-01-03')
          AND (A.timestamp) < (TIMESTAMP '2025-01-17')
        )
      )
  )
SELECT
  COUNT(DISTINCT page_event.ur_normalized) AS "page_event.distinct_user_count"
FROM
  page_event
WHERE
  (
    (
      (page_event.timestamp) >= (TIMESTAMP '2025-01-03')
      AND (page_event.timestamp) < (TIMESTAMP '2025-01-17')
    )
  )
  AND (page_event.ns_tenant_id) = 2683
  AND (
    1 = 1
    AND
    1 = 1
    AND
    1 = 1
    AND
    1 = 1
    AND
    1 = 1
    AND

    1 = 1
  )
LIMIT
  500