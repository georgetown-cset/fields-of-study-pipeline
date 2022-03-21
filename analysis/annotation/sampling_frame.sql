with frame as (
  select
    merged_id,
    'zh' as lang,
  from staging_fields_of_study_v2.zh_corpus  -- pubs since last Airflow run
  union all
  select
    merged_id,
    'zh' as lang,
  from staging_fields_of_study_v2.prev_zh_corpus  -- pubs processed in last Airflow run
  union all
  select
    merged_id,
    'en' as lang,
  from staging_fields_of_study_v2.en_corpus  -- pubs since last Airflow run
  union all
  select
    merged_id,
    'en' as lang,
  from staging_fields_of_study_v2.prev_en_corpus  -- pubs processed in last Airflow run
)
select
  merged_id,
  -- The row_number() within publication year provides an integer ID for sampling. Hashing these with
  --     FarmHash yields an INT64 (a signed 64-bit int). Keep in mind this isn't deterministic because
  --     row order isn't stable in BQ
  farm_fingerprint(cast(row_number() over (partition by year) as string)) as id_hash,
  lang,
  year,
from frame
-- Get year
left join gcp_cset_links_v2.corpus_merged using(merged_id)
