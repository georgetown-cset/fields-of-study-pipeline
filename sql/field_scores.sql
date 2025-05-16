-- The final field_score table is the union of the predicted scores for publications with EN text
-- and the imputed scores for publications that don't have EN text but do have a reference with EN
-- text
with merged as (
  select
    merged_id,
    fields,
    false as is_imputed
  from {{staging_dataset}}.en_scores
  union all
  select
    merged_id,
    -- We unnest and nest again so that the structs match; imputed_scores has more nested fields
    array_agg(struct(
      field.name,
      field.score
      )) as fields,
    true as is_imputed
  from {{staging_dataset}}.imputed_scores, unnest(fields) as field
  group by merged_id
)

select *
from merged
where merged_id in (
  select merged_id
  from literature.sources
)