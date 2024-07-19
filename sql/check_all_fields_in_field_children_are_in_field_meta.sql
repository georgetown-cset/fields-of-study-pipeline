-- All the fields in the field_hierarchy table should also appear in the field_meta table
select
    count(display_name) = 0
from
  {{staging_dataset}}.field_hierarchy
where
  display_name not in (
    select name
    from {{staging_dataset}}.field_meta
)
