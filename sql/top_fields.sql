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
    trim(unnested.name) as name,
    level as field_level,
    field_score,
    row_number() over (partition by merged_id, level order by field_score desc) as field_rank
  from unnested
  inner join {{staging_dataset}}.field_meta on trim(unnested.name) = trim(field_meta.name)
),

top_l1_fields as (
  select *
  from field_ranks
  where 
    field_level = 1
    and field_rank <= 5
),

l2_candidates as (
  select
    top_l1_fields.merged_id,
    field_ranks.name,
    field_ranks.field_level,  # This will always be 2 or 3, because of the series of inner joins below
    field_ranks.field_score,
    # Rerank the L2s that are children of the top L1s
    row_number() over (partition by top_l1_fields.merged_id, field_ranks.field_level order by field_ranks.field_score desc) as field_rank
  from top_l1_fields
  # Get the L2 children of the top L1 fields; this is just a taxonomy lookup
  inner join {{staging_dataset}}.field_hierarchy
    on trim(top_l1_fields.name) = trim(field_hierarchy.display_name)
  # Get the scores for the children of the top L1 fields
  inner join field_ranks
    # For each merged_id ...
    on field_ranks.merged_id = top_l1_fields.merged_id
    # We want the scores of the fields that are L2 or L3 children of the top L1 fields
    and trim(field_ranks.name) = trim(field_hierarchy.child_display_name)
),

# After re-ranking the subset of L2 fields, we can restrict on the top L2s in the subset
top_l2_fields as (
  select *
  from l2_candidates
  where field_rank = 1
  order by merged_id, field_level, field_rank 
),

combined as (
  # Combine the two sets of levels
  select * 
  from field_ranks 
  where field_level between 0 and 1
  and field_rank = 1
  union all 
  select * from top_l2_fields
)

select
  merged_id,
  array_agg(
    struct(
      name,
      field_level as level,
      field_score as score)
    order by field_level) fields
  from combined
  group by merged_id