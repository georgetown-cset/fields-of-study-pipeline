-- All the fields in the field_children table should also appear in the field_meta table
select
  count(*) = 0
from
  {{staging_dataset}}.field_children
where
  parent_name not in (
    select name
    from {{staging_dataset}}.field_meta
  )
  or child_name not in (
    select name
    from {{staging_dataset}}.field_meta
  )
