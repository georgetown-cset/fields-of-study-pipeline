with unnested as (
  select
    merged_id,
    field.name,
    field.score as field_score
  from {{staging_dataset}}.field_scores, unnest(fields) as field
),

field_ranks as (
  select
    merged_id,
    name,
    level as field_level,
    field_score,
    row_number() over (partition by merged_id, level order by field_score desc) as field_rank
  from unnested
  inner join {{staging_dataset}}.field_meta using(name)
)

select
  merged_id,
  array_agg(distinct name) level_one_fields
from field_ranks
where field_rank <= 5
and field_level = 1
group by merged_id
