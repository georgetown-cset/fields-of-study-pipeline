import pandas as pd

from fos.settings import ASSETS_DIR
from fos.util import preprocess

SQL = """\
with fields as (
  select 
    FieldOfStudyId,
    DisplayName
  from gcp_cset_mag.FieldsOfStudy
  where Level = 0
),
scores as (
  select 
    PaperId,
    FieldOfStudyId,
    DisplayName,
    score,
    row_number() over(partition by FieldOfStudyId order by Score desc) as score_rank
  from gcp_cset_mag.PaperFieldsOfStudy
  inner join fields using(FieldOfStudyId)
  inner join gcp_cset_mag.PapersWithAbstracts using(PaperId)
  where 
    OriginalTitle is not null 
    and abstract is not null
)
select 
  PaperId as mag_id,
  FieldOfStudyId as field_id,
  DisplayName as display_name,
  score,
  score_rank,
  OriginalTitle as title,
  abstract
from scores
inner join gcp_cset_mag.PapersWithAbstracts using(PaperId)
where 
  score_rank <= 100
order by 
  DisplayName,
  score_rank
"""


def main():
    df = pd.read_gbq(SQL, project_id='gcp-cset-projects')
    df['text'] = df.apply(lambda row: preprocess(row['title'] + ' ' + row['abstract']), axis=1)
    df.to_pickle(ASSETS_DIR / 'fields/example_text.pkl.gz')


if __name__ == '__main__':
    main()
