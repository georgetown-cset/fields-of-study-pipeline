with frame as (
  select merged_id
  from staging_fields_of_study_v2.zh_corpus  -- pubs since last Airflow run
  union all
  select merged_id
  from staging_fields_of_study_v2.prev_zh_corpus  -- pubs processed in last Airflow run
)
select
  merged_id,
  -- The row_number() within publication year provides an integer ID for sampling. Hashing these with
  --     FarmHash yields an INT64 (a signed 64-bit int). Keep in mind this isn't deterministic because
  --     row order isn't stable in BQ
  farm_fingerprint(cast(row_number() over (partition by year) as string)) as id_hash,
from gcp_cset_links_v2.corpus_merged
inner join frame using(merged_id)
where
  year >= 2010
  and year <= 2021