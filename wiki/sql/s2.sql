with matches as (
  select
    wiki_references.*,
    pid,
  from field_model_replication.wiki_references
  left join gcp_cset_semantic_scholar.gorc_metadata on cast(id_value as int64) = pid
  where id_type = 's2'
)
select
  pid is not null as in_gorc,
  count(*)
from matches
group by in_gorc