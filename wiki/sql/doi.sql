-- Get database IDs for observed DOIs
with matches as (
  select distinct
    refs.id,
    meta.id as orig_id,
  from field_model_replication.wiki_references refs
    -- We get the DOIs associated with publications here
  left join gcp_cset_links_v2.all_metadata_with_cld2_lid meta on meta.clean_doi = refs.id_value
  where id_type = 'doi'
),
-- Available EN text
en_corpus as (
    select
      merged_id,
      text
    from staging_fields_of_study_v2.en_corpus
    union distinct
    select
      merged_id,
      text
    from staging_fields_of_study_v2.prev_en_corpus
),
-- Available ZH text
zh_corpus as (
    select
      merged_id,
      text
    from staging_fields_of_study_v2.zh_corpus
    union distinct
    select
      merged_id,
      text
    from staging_fields_of_study_v2.prev_zh_corpus
)
--
select distinct
  matches.id,
  merged_id,
  en_corpus.text en_text,
  zh_corpus.text zh_text,
from matches
-- Walk orig_id -> merged_id
inner join gcp_cset_links_v2.article_links using(orig_id)
-- Given merged_id, best EN text if any
left join en_corpus using(merged_id)
-- Similarly, best ZH if any
left join zh_corpus using(merged_id)
where
  en_corpus.text is not null
  or zh_corpus.text is not null