select
  logical_and(field_children.parent_name is not null),
  logical_and(field_hierarchy.display_name is not null)
from {{staging_dataset}}.field_children
full outer join {{staging_dataset}}.field_hierarchy
  on field_children.parent_name = field_hierarchy.display_name
