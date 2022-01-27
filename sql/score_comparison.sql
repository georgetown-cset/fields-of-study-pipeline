-- Compare FieldModel scores with MAG scores
select
  PaperId,
  PaperFieldsOfStudy.FieldOfStudyId,
  sample_output_fields.score as sample_score,
  PaperFieldsOfStudy.Score as mag_score,
  round(abs(sample_output_fields.score - PaperFieldsOfStudy.Score), 4) as score_diff
from fields_of_study.mag_sample_output
cross join unnest(mag_sample_output.fields) as sample_output_fields
left join gcp_cset_mag.PaperFieldsOfStudy
  on PaperFieldsOfStudy.PaperId = cast(mag_sample_output.id as string)
    and PaperFieldsOfStudy.FieldOfStudyId = cast(sample_output_fields.id as string)
