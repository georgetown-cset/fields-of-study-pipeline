-- Merge any new scores with the old ones in the production dataset; if we have scores for
-- an id in both tables (which can happen if the text for that id changed), take the new score
with old_scores as (
  select
    merged_id,
    fields
  from
    {{staging_dataset}}.field_scores
  where
    merged_id not in (
      select
        id
      from
        {{staging_dataset}}.new_en_scores
    ) and merged_id in (
      select
        merged_id
      from
        {{staging_dataset}}.en_corpus
    )
),
new_scores as (
  select
    new_en_scores.id as merged_id,
    -- Unnest and nest in this order to avoid mismatched struct error
    array_agg(struct(field.id, field.score)) as fields
  from {{staging_dataset}}.new_en_scores,
    unnest(fields) as field
  group by
    new_en_scores.id
)
select
  merged_id,
  fields
from new_scores
union all
select
  merged_id,
  fields
from
  old_scores
