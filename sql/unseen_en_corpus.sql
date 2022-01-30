-- Use to filter out articles whose metadata hasn't changed since the last run (so we can still use
-- our predictions)
with still_valid_merged_ids as (
  select
    prev_en_corpus.merged_id
  from
    {{staging_dataset}}.prev_en_corpus
  inner join
    {{staging_dataset}}.en_corpus
    on prev_en_corpus.merged_id = en_corpus.merged_id
      and prev_en_corpus.text = en_corpus.text
  -- Only consider a merged ID "still valid" if we have a prediction for it
  -- and it isn't an imputed prediction (though this is unlikely)
  where prev_en_corpus.merged_id in (
    select merged_id
    from {{staging_dataset}}.field_scores
    where not is_imputed
  )
)
select
  *
from
  {{staging_dataset}}.en_corpus
where
  merged_id not in (
    select merged_id
    from still_valid_merged_ids
  )
