with counts as (
  select count(*) as n
  from {{staging_dataset}}.field_children
  union all
  select count(*) as n
  from {{staging_dataset}}.field_hierarchy
)

select
  count(distinct n) = 1
from counts