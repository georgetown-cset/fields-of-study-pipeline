with counts as (
  select count(*) as n
  from staging_fields_of_study_v2.field_children
  union all
  select count(*) as n
  from staging_fields_of_study_v2.field_hierarchy
)

select
  count(distinct n) = 1
from counts