-- Get database IDs for observed DOIs
with matches as (
  select
    refs.id,
    meta.id as orig_id,
  from field_model_replication.wiki_references_temp refs
    -- We get the DOIs associated with publications here
  left join staging_literature.all_metadata_norm meta on meta.clean_doi = refs.id_value
  where id_type = 'doi'
)
select
  DISTINCT
  matches.id,
  merged_id,
  COALESCE(title_english, "") || " " || COALESCE(abstract_english, "") as text,
from matches
-- Walk orig_id -> merged_id
inner join literature.sources using(orig_id)
-- Given merged_id, get best EN title and abstract
-- We already have language-specific results in the 'en_corpus' and 'zh_corpus' tables, but many refs from ZH wikipedia
-- are to EN papers.
inner join literature.papers using(merged_id)