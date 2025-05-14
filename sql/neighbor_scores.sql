with no_pre_scored_neighbors as (
  select
    neighbors.merged_id,
    neighbor_id
  from {{staging_dataset}}.neighbors
  left join (
    select merged_id
    from {{staging_dataset}}.en_scores
  ) as en_scores using (merged_id)
  where en_scores.merged_id is null
)

-- The papers without scores are non-EN or don't have title/abstract.
-- Our approach is to find any neighbors of theirs in the citation
-- graph that do have scores
select
  no_pre_scored_neighbors.merged_id,
  neighbor_id,
  fields
-- For publications without field scores (see WHERE)
from (
  select distinct
    merged_id,
    neighbor_id
  from no_pre_scored_neighbors
) as no_pre_scored_neighbors
-- And get any scores associated (at this point any pubs
-- whose neighbors don''t have scores drop out)
inner join {{staging_dataset}}.en_scores neighbor_scores
  on neighbor_scores.merged_id = neighbor_id
where
  -- A small number of papers that go through the field model don''t
  -- receive any non-negative scores, and these may appear in the
  -- en_zh_scores table, so exclude them here
  neighbor_scores.fields is not null