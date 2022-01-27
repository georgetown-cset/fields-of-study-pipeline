select
  cast(FieldOfStudyId as int64) as field_id,
  DisplayName as name,
  Level as level,
  CreatedDate as created_date
from gcp_cset_mag.FieldsOfStudy
