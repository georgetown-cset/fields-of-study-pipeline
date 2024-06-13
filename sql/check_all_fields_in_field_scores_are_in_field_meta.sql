-- If a field_id is associated with a paper, it should appear in our field metadata table.
-- We copy the metadata from gcp_cset_mag while the field_ids in the field_scores table are
-- from the field model, so this will fail when MAG has deployed an updated model and we
-- haven't.
select
    count(field.display_name) = 0
from
  {{staging_dataset}}.field_scores, unnest(fields) as field
where
  field.display_name not in (
    select display_name
    from {{staging_dataset}}.field_meta
)
