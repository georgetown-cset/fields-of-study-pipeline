select
  min(field.score) >= 0.0 as min_score,
  max(field.score) <= 1.0 as max_score,
from {{staging_dataset}}.en_scores, unnest(fields) as field
