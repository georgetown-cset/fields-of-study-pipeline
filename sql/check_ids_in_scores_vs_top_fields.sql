with scores as (
  select merged_id
  from {{staging_dataset}}.en_scores
  union distinct
  select merged_id
  from {{staging_dataset}}.imputed_scores
)
select
  logical_and(scores.merged_id is not null),
  logical_and(top_fields.merged_id is not null)
from scores
full outer join {{staging_dataset}}.top_fields
  on scores.merged_id = top_fields.merged_id
