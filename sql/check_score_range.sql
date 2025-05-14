select
  min(field.score) >= 0.0 as min_score,
  max(field.score) <= 1.0 as max_score,
from staging_fields_of_study_v2.en_scores, unnest(fields) as field
