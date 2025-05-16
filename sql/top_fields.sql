create or replace table staging_fields_of_study_v2.top_fields

cluster by
  top_l0,
  top_l1,
  top_l2,
  top_l3

as (

  with unnested as (
    select
      merged_id,
      field.name,
      field.score,
    from staging_fields_of_study_v2.field_scores, unnest(fields) as field
    where
      -- Omit zero scores
      field.score > 0.0
  ),

  ranked as (
    select
      merged_id,
      name,
      level,
      score,
      row_number() over (partition by merged_id, level order by score desc) as field_rank
    from unnested
    inner join staging_fields_of_study_v2.field_meta using(name)
  )

  select
    merged_id,
    array_agg(
      struct(
        name,
        level,
        score)
      order by level, score desc) as fields,
      any_value(if(level = 0 and field_rank = 1, name, null)) top_l0,
      any_value(if(level = 1 and field_rank = 1, name, null)) top_l1,
      any_value(if(level = 2 and field_rank = 1, name, null)) top_l2,
      any_value(if(level = 3 and field_rank = 1, name, null)) top_l3,
      array_agg(if(level = 0, name, null) ignore nulls order by score desc) top_3_l0,
      array_agg(if(level = 1, name, null) ignore nulls order by score desc) top_3_l1,
      array_agg(if(level = 2, name, null) ignore nulls order by score desc) top_3_l2,
      array_agg(if(level = 3, name, null) ignore nulls order by score desc) top_3_l3,
  from ranked
  where field_rank <= 3
  group by merged_id

)