-- Create a slightly different version of the field_children table introduced in mid-2024; both are in use
select
  field_children.parent_name as normalized_name,
  field_children.parent_name as display_name,
  parent_meta.level as parent_level,
  field_children.child_name as child_normalized_name,
  field_children.child_name as child_display_name,
  child_meta.level as child_level,
from {{staging_dataset}}.field_children
inner join {{staging_dataset}}.field_meta as parent_meta
  on field_children.parent_name = parent_meta.name
inner join {{staging_dataset}}.field_meta as child_meta
  on field_children.child_name = child_meta.name
