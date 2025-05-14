with n_neighbors as (
  -- How many neighbors with 1+ field score (any field) does each of these papers have?
  select distinct
    merged_id,
    count(distinct neighbor_id) as n
  from {{staging_dataset}}.neighbor_scores
  group by merged_id
),

observed_fields as (
  -- Summarize neighbors' observed field scores. This is
  -- the first step in imputation. Below we weight the
  -- resulting averages
  select
    merged_id,
    field.name,
    -- Take the field average over the observed scores
    avg(field.score) as avg_neighbors_score,
    -- How many references with a score for this field?
    count(distinct neighbor_id) as neighbors_with_field_count
  from {{staging_dataset}}.neighbor_scores, unnest(fields) as field
  group by
    merged_id,
    field.name
),

unnested_imputations as (
  select
    -- For each publication-field pair (for the fields we observe among a
    -- publication's references) ...
    observed_fields.merged_id,
    observed_fields.name,
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

-- Reshape for consistency with en_scores
select
  merged_id,
  neighbors_count,
  array_agg(struct(
    name,
    avg_neighbors_score,
    neighbors_with_field_count,
    score
    )) as fields
from unnested_imputations
group by
  merged_id,
  neighbors_count
