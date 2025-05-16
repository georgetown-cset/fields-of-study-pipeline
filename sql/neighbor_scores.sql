-- This query identifies publications that (1) didn't receive field scores and (2) have citation-network neighbors that
-- did receive field scores. It joins this set of papers with the field scores of their neighbors.

with neighbors as (
  -- This CTE gives the citation-network neighbors of every publication
  select
    merged_id,
    ref_id as neighbor_id
  from literature.references
  -- Neighbors are defined by having either an in-citation or out-citation relation
  union distinct
  select
    ref_id as merged_id,
    merged_id as neighbor_id
  from literature.references
),

unscored_neighbors as (
  -- This CTE identifies the publications that have 1+ citation neighbor, and didn't receive field scores. These are
  -- papers that don't have EN title text and don't have EN abstract text.
  select
    neighbors.merged_id,
    neighbors.neighbor_id
  from neighbors
  where neighbors.merged_id not in (
    select en_scores.merged_id
    from staging_fields_of_study_v2.en_scores
  )
)

-- We join the unscored publications with their scored neighbors, if any. Aggregation over these scores happens in
-- imputed_scores.sql.
select
  unscored_neighbors.merged_id,
  unscored_neighbors.neighbor_id,
  en_scores.fields
from unscored_neighbors
-- The inner join drops any pubs whose neighbors don't have field scores
inner join staging_fields_of_study_v2.en_scores
  on unscored_neighbors.neighbor_id = en_scores.merged_id
where
  -- A small number of papers that go through the field model don't receive any non-negative scores; exclude these
  en_scores.fields is not null
