select
  cast(FieldOfStudyId as int64) as field_id,
  cast(ChildFieldOfStudyId as int64) as child_field_id
from gcp_cset_mag.FieldOfStudyChildren
