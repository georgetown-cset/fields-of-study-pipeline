select
  PaperId,
  -- We require non-null titles and abstracts below, so this is safe
  pwa.OriginalTitle || '. ' || pwa.abstract as text
from gcp_cset_mag.PapersWithAbstracts pwa
inner join gcp_cset_links_v2.all_metadata_with_cld2_lid on PaperId = id
where
  -- Require both title and abstract
  pwa.OriginalTitle is not null
  and pwa.abstract is not null
  -- No patents, datasets, or dissertations
  and pwa.DocType in ('Journal', 'Conference')
  -- English titles and English abstracts
  and ((
      title_cld2_lid_success is true
      and title_cld2_lid_is_reliable is true
    ) or (
      abstract_cld2_lid_success is true
      and abstract_cld2_lid_is_reliable is true
  ))
  and abstract_cld2_lid_first_result = 'ENGLISH'
  and title_cld2_lid_first_result = 'ENGLISH'
--   58.7M is the size of the PapersWithAbstracts table, so if we weren't
--   doing the joining and filtering above, this would give us ~110K results
  and rand() < 110000 / 58712333
-- Get exactly 100K papers
limit 100000
