with unnested as (
  select
    merged_id,
    field.name,
    field.score as field_score
  from staging_fields_of_study_v2.field_scores, unnest(fields) as field
),

field_ranks as (
  select
    merged_id,
    name,
    level as field_level,
    field_score,
    row_number() over (partition by merged_id, level order by field_score desc) as field_rank
  from unnested
  inner join staging_fields_of_study_v2.field_meta using(name)
),

limit_children as (
  select
    field_ranks.merged_id,
    name
  from field_ranks
  inner join staging_fields_of_study_v2.field_hierarchy
    on name = child_display_name
  inner join staging_fields_of_study_v2.parent_fields
  on (field_ranks.merged_id = parent_fields.merged_id and level_one_field = display_name)
  where field_level = 2 or field_level = 3
)

select
  merged_id,
  array_agg(
    struct(
      field_ranks.name,
      field_level as level,
      field_score as score)
    order by field_level) fields
from field_ranks
left join limit_children using(merged_id)
where field_rank = 1
and limit_children.name is not null
group by merged_id
