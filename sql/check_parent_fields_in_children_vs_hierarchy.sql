select
  logical_and(field_children.parent_name is not null),
  logical_and(field_hierarchy.display_name is not null)
from staging_fields_of_study_v2.field_children
full outer join staging_fields_of_study_v2.field_hierarchy
  on field_children.parent_name = field_hierarchy.display_name
