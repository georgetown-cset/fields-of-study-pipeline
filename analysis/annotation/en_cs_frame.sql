with top_fields as (
  select
    merged_id,
    field_id,
    row_number() over(partition by merged_id, level order by score desc) as rank
  from `gcp-cset-projects`.fields_of_study_v2.field_scores, unnest(fields) as field
  inner join `gcp-cset-projects`.fields_of_study_v2.field_meta on field.id = field_id
  where level = 0
),
cs as (
  select
    merged_id,
  from top_fields
  where rank <= 2
    and field_id = 41008148 -- CS
)
select
  merged_id,
  id_hash
from `gcp-cset-projects`.field_model_replication.sampling_frame
inner join cs using(merged_id)
where lang = 'en'