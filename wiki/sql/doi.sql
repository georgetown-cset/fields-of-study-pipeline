-- Get database IDs for observed DOIs
with matches as (
  select
    refs.id,
    meta.id as orig_id,
  from field_model_replication.wiki_references refs
    -- We get the DOIs associated with publications here
  left join gcp_cset_links_v2.all_metadata_with_cld2_lid meta on meta.clean_doi = refs.id_value
  where id_type = 'doi'
)
select
  matches.id,
  merged_id,
  text,
from matches
-- Walk orig_id -> merged_id
inner join gcp_cset_links_v2.article_links using(orig_id)
-- Given merged_id, get best EN title and abstract
-- We could use corpus_merged, but we already have language-specific results in the 'en_corpus' and 'zh_corpus' tables
-- ... on the other hand, many refs from ZH wikipedia may be to EN papers. We can drop those or translate them I suppose
-- inner join gcp_cset_links_v2.corpus_merged using(merged_id)
inner join field_model_replication.en_corpus using(merged_id)