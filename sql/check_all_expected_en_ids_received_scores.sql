select
  count(merged_id) = 0
from
  {{staging_dataset}}.unseen_en_corpus
where
  merged_id not in (
    select
      id
    from
      {{staging_dataset}}.new_en_scores
  )
