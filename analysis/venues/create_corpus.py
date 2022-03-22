"""
Get text for top-venue papers.
"""
from pathlib import Path

import pandas as pd

from fos.gcp import write_query


def main():
    table = "field_model_replication.top_venue_papers"
    write_query(Path("corpus.sql"), destination="field_model_replication.top_venue_papers", clobber=True)
    print(pd.read_gbq(f"select count(*) from {table}", project_id='gcp-cset-projects'), "rows")

    df = pd.read_gbq(f"select * from {table} where year >= 2010", project_id='gcp-cset-projects')
    df.rename(columns={"paper_id": "id"}, inplace=True)

    print(df.year.value_counts())
    print(df.journal_name.value_counts())
    df.to_json("ai_venue_text.jsonl", orient="records", lines=True)


if __name__ == '__main__':
    main()
