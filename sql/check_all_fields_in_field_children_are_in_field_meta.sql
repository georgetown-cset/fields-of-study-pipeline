-- All the fields in the field_children table should also appear in the field_meta table
select
    count(field_id) = 0
from
  {{staging_dataset}}.field_children
where
  field_id not in (
    select field_id
    from {{staging_dataset}}.field_meta
)
