select (select
  count(merged_id) = 0
from
  {{staging_dataset}}.en_corpus
where
  merged_id not in (
    select
      merged_id
    from
      {{staging_dataset}}.new_en
  )
)