with scores as (
  select merged_id
  from staging_fields_of_study_v2.en_scores
  union distinct
  select merged_id
  from staging_fields_of_study_v2.imputed_scores
)
select
  logical_and(scores.merged_id is not null),
  logical_and(top_fields.merged_id is not null)
from scores
full outer join staging_fields_of_study_v2.top_fields
  on scores.merged_id = top_fields.merged_id
