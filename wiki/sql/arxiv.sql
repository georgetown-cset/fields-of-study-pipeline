with matches as (
  select
    refs.*,
    arxiv.id as arxiv_id,
  from field_model_replication.wiki_references refs
  left join gcp_cset_arxiv_metadata.arxiv_metadata_latest arxiv on arxiv.id = refs.id_value
  where id_type = 'arxiv'
)
select
  arxiv_id is not null as in_arxiv,
  count(*)
from matches
group by in_arxiv