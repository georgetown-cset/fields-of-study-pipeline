select
  scholar_cat,
  PaperId as paper_id,
  OriginalTitle as title,
  abstract,
  Year as year,
from `gcp-cset-projects.field_model_replication.top_mag_venues`
inner join `gcp-cset-projects.gcp_cset_mag.Journals` on DisplayName = mag_name
inner join `gcp-cset-projects.gcp_cset_mag.PapersWithAbstracts` using(JournalId)