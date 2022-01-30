with neighbors as (
  -- The paper_references_merged table is an edge list
  -- giving paper, reference pairs. We union it with
  -- these same pairs in the reverse order to get a lookup
  -- table of citation graph neighbors
  select
    merged_id,
    ref_id as neighbor_id
  from gcp_cset_links_v2.paper_references_merged
  union distinct
  select
    ref_id as merged_id,
    merged_id as neighbor_id
  from gcp_cset_links_v2.paper_references_merged
),
neighbor_scores as (
  -- The papers without scores are non-EN/ZH or don't have title/abstract.
  -- Our approach is to find any neighbors of theirs in the citation
  -- graph that do have scores
  select
    article_links_nested.merged_id,
    neighbors.neighbor_id,
    neighbor_scores.fields
  -- For publications without field scores (see WHERE)
  from gcp_cset_links_v2.article_links_nested
  -- Take the IDs of all their neighbors (at this point any
  -- pubs not in the citation graph drop out)
  inner join neighbors using(merged_id)
  -- And get any scores associated (at this point any pubs
  -- whose neighbors don't have scores drop out)
  inner join {{staging_dataset}}.en_zh_scores neighbor_scores
    on neighbor_scores.merged_id = neighbors.neighbor_id
  -- We exclude papers that already have scores
  left join {{staging_dataset}}.en_zh_scores
    on en_zh_scores.merged_id = article_links_nested.merged_id
  -- The more readable 'where not in (subquery)' approach gives an OOM
  -- error, so we do left join + where
  where
    -- As explained immediately above exclude paprers that already
    -- have scores
    en_zh_scores.merged_id is null
    -- A small number of papers that go through the field model don't
    -- receive any non-negative scores, and these may appear in the
    -- en_zh_scores table, so exclude them here
    and neighbor_scores.fields is not null
),
n_neighbors as (
  -- How many neighbors with 1+ field score (any field) does each of these papers have?
  select distinct
    merged_id,
    count(distinct neighbor_id) as n
  from neighbor_scores
  group by merged_id
),
observed_fields as (
  -- Summarize neighbors' observed field scores. This is
  -- the first step in imputation. Below we weight the
  -- resulting averages
  select
    merged_id,
    field.id as field_id,
    -- Take the field average over the observed scores
    avg(field.score) as avg_neighbors_score,
    -- How many references with a score for this field?
    count(distinct neighbor_id) as neighbors_with_field_count
  from neighbor_scores, unnest(fields) as field
  group by
    merged_id,
    field.id
),
unnested_imputations as (
  --
  select
    -- For each publication-field pair (for the fields we observe among a
    -- publication's references) ...
    observed_fields.merged_id,
    observed_fields.field_id,
    -- We have the average over the observed scores for a field;
    round(observed_fields.avg_neighbors_score, 4) as avg_neighbors_score,
    -- We have the count of the neighbors for which a non-negative score
    -- is observed
    observed_fields.neighbors_with_field_count,
    -- And the count of neighbors with any field score
    n_neighbors.n as neighbors_count,
    -- The average over the references' observed scores *and* imputed
    -- zeroes for references where we don't observe scores for a field
    -- is the frequency-weighted average of the observed scores and the
    -- imputed zeroes
    round(observed_fields.avg_neighbors_score *
      (observed_fields.neighbors_with_field_count / n_neighbors.n), 4) as score
  from observed_fields
  inner join n_neighbors using (merged_id)
)
-- Reshape for consistency with en_zh_scores
select
  merged_id,
  neighbors_count,
  array_agg(struct(
    field_id as id,
    avg_neighbors_score,
    neighbors_with_field_count,
    score
    )) as fields
from unnested_imputations
group by
  merged_id,
  neighbors_count
