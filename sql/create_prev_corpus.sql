-- Create an empty prev_en_corpus for use in a cold start
CREATE TABLE IF NOT EXISTS staging_fields_of_study_v2.prev_en_corpus (
  merged_id string NOT NULL,
  text string NOT NULL
)