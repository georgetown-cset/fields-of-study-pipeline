WITH
  fields_minus_zero AS (
  SELECT DISTINCT
    child_normalized_name AS normalized_name,
    child_display_name AS name,
    child_level AS level
  FROM
    staging_fields_of_study_v2.field_hierarchy),

  level_zero_fields AS (
  SELECT DISTINCT
    normalized_name,
    display_name as name,
    parent_level AS level
  FROM
    staging_fields_of_study_v2.field_hierarchy
  WHERE
    parent_level = 0)

SELECT DISTINCT
  *
FROM
  fields_minus_zero
UNION DISTINCT
SELECT DISTINCT
  *
FROM
  level_zero_fields