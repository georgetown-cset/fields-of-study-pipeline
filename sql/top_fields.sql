with unnested as (
  select
    merged_id,
    field.id as field_id,
    field.score as field_score
  from {{staging_dataset}}.field_scores, unnest(fields) as field
),
field_ranks as (
  select
    merged_id,
    field_id,
    name as field_name,
    level as field_level,
    field_score,
    row_number() over (partition by merged_id, level order by field_score desc) as field_rank
  from unnested
  inner join {{staging_dataset}}.field_meta using(field_id)
)
select
  merged_id,
  array_agg(
    struct(
      field_id as id,
      field_name as name,
      field_level as level,
      field_score as score)
    order by field_level) fields
from field_ranks
where field_rank = 1
group by merged_id
