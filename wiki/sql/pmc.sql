-- Get database IDs for observed DOIs
with matches as (
  select
    refs.id,
    works.id as orig_id,
  from field_model_replication.wiki_references_temp refs
    -- We get the PMIDs associated with publications here
    -- Note that openalex prepends a long url that we have to remove with substring
  left join openalex.works on "PMC" || substring(works.ids.pmcid, 43) = refs.id_value
  where id_type = 'pmc'
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