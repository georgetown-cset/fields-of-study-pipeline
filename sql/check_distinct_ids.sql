-- check no duplicate ids
select
  count(distinct(merged_id)) = count(merged_id)
from {{staging_dataset}}.field_scores
