"""
Draw a sample from papers published in top venues.
"""
import os

import pandas as pd

try:
    os.chdir("analysis/venues")
except FileNotFoundError:
    pass


def main():
    docs = pd.read_json("venue_text.jsonl", nrows=100, lines=True)
    docs.head()
    venues = pd.read_gbq("select scholar_cat, paper_id from field_model_replication.top_venue_papers "
                         "tablesample system (5 percent)",
                         project_id='gcp-cset-projects')
    venues['scholar_cat'].value_counts()
    ai_ = pd.read_gbq("select count(*) from field_model_replication.top_venue_papers "
                "where scholar_cat = 'eng_artificialintelligence'")


if __name__ == '__main__':
    main()
