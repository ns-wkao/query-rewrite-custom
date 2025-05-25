SELECT
  A.*
, SPLIT(A.organization_unit, '/') OU
, CONCAT(A.app, A.instance_id) ns_app_instance
FROM
  "redshift_poc_iceberg"."mv_alert_v3_alias_derived" A
WHERE ((A.ns_tenant_id = 2683) AND ((A.timestamp >= TIMESTAMP '2025-01-03') AND (A.timestamp < TIMESTAMP '2025-01-17')))