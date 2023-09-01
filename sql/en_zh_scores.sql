-- Merge any new scores with the old ones in the production dataset; if we have scores for
-- an id in both tables (which can happen if the text for that id changed), take the new score
with in_lang_ids as (
  select
    merged_id,
  from staging_literature.all_metadata_with_cld2_lid
  -- Get merged_id
  inner join literature.sources on article_links.orig_id = all_metadata_with_cld2_lid.id
  where
  -- This just shrinks the results a bit (to publications with en/zh titles or abstracts)
  (
    (title is not null
      and title_cld2_lid_success is true
      and title_cld2_lid_is_reliable is true
      and lower(title_cld2_lid_first_result_short_code) in ("en", "zh")) is true
    or (abstract is not null
      and abstract_cld2_lid_success is true
      and abstract_cld2_lid_is_reliable is true
      and lower(abstract_cld2_lid_first_result_short_code) in ("en", "zh")) is true
  )
),
old_scores as (
  select
    merged_id,
    fields
  from
    {{production_dataset}}.field_scores
  where
    merged_id not in (
      select
        merged_id
      from
        {{staging_dataset}}.new_en_zh_scores
    ) and merged_id in (
      select
        merged_id
      from
        in_lang_ids
    )
)
select
  merged_id,
  array_agg(struct(field.id, field.score)) as fields
from {{staging_dataset}}.new_en_zh_scores,
unnest(fields) as field
group by merged_id
union all
select
  merged_id,
  fields
from
  old_scores
