with frame as (
  select merged_id
  from staging_fields_of_study_v2.en_corpus  -- EN pubs since last Airflow run
  union all
  select merged_id
  from staging_fields_of_study_v2.prev_en_corpus  -- EN pubs processed in last Airflow run
)


-- Get IDs and publication years for papers in the merged corpus that were published 2010-2021 and
--   have an English title or abstract. The latter is important because the MATCH embeddings are
--   English language, and it also should mean that they have field scores directly from the MAG
--   field model (not imputed), making their labels a bit more reliable
select
  merged_id,
  year,
  -- The row_number() within publication year provides an integer ID for sampling. Hashing these with
  --     FarmHash yields an INT64 (a signed 64-bit int). Keep in mind this isn't deterministic because
  --     row order isn't stable in BQ
  farm_fingerprint(cast(row_number() over (partition by year) as string)) as id_hash,
from gcp_cset_links_v2.corpus_merged
-- We need to restrict the corpus to the set with field scores in order to get the expected sample size
inner join frame using(merged_id)
where
  year >= 2010
  and year <= 2021
