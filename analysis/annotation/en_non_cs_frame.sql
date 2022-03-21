select
  merged_id,
  id_hash
from `gcp-cset-projects`.field_model_replication.sampling_frame
where lang = 'en'
  and merged_id not in (select merged_id from field_model_replication.en_cs_sampling_frame)