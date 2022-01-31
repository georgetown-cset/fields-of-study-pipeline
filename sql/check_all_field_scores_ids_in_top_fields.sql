select
  count(merged_id) = 0
from
  {{staging_dataset}}.field_scores
where
  fields is not null
  and merged_id not in (
    select merged_id
    from {{staging_dataset}}.top_fields
  )
