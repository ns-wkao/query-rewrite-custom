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
          (A.TIMESTAMP >= TIMESTAMP '2025-01-03')
          AND (A.TIMESTAMP < TIMESTAMP '2025-01-17')
        )
      )
  )
SELECT
  COUNT(DISTINCT page_event.ur_normalized) "page_event.distinct_user_count"
FROM
  (
    (
      page_event
      LEFT JOIN "redshift_poc_iceberg"."usergroups" groups ON (
        (page_event.ns_tenant_id = groups.ns_tenant_id)
        AND (LOWER(page_event.user) = LOWER(groups.group_user))
      )
    )
    CROSS JOIN UNNEST (groups.groupnames) T (usergroups)
  )
WHERE
  (
    (
      (page_event.TIMESTAMP >= TIMESTAMP '2025-01-03')
      AND (page_event.TIMESTAMP < TIMESTAMP '2025-01-17')
    )
    AND (UPPER(usergroups) = UPPER('web'))
    AND (page_event.ns_tenant_id = 2683)
    AND (
      (1 = 1)
      AND (1 = 1)
      AND (1 = 1)
      AND (1 = 1)
      AND (1 = 1)
      AND (1 = 1)
      AND (1 = 1)
    )
  )
LIMIT
  500