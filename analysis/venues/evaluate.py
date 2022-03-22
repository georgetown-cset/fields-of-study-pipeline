"""
Evaluate top fields for pubs from top venues by field.
"""

import pandas as pd

from fos.settings import ASSETS_DIR

try:
    import os

    os.chdir('analysis/venues')
except FileNotFoundError:
    pass

def main():
    meta = pd.read_pickle(ASSETS_DIR / "fields/fos.pkl.gz")
    meta.index = meta.index.astype(int)
    id_to_name = meta.query("level == 0")["display_name"].to_dict()

    mag = pd.read_json('ai_venue_text_mag_scores.jsonl', lines=True)
    cset = pd.read_json('ai_venue_text_cset_scores.jsonl', lines=True)
    venues = pd.read_json('ai_venue_text.jsonl', lines=True)
    del venues['text']

    mag = pd.merge(mag, venues, on='id', how='inner')
    mag = extract_top_field(mag, id_to_name)
    mag_fields = summarize_top_field(mag)

    cset = pd.merge(cset, venues, on='id', how='inner')
    cset = extract_top_field(cset, id_to_name)
    cset_fields = summarize_top_field(cset)

    venues[['scholar_cat', 'journal_name']].drop_duplicates()

    field_counts = pd.merge(mag_fields, cset_fields, suffixes=('_mag', '_cset'), left_index=True, right_index=True)
    field_counts.rename(columns=lambda x: x.replace('id_', 'n_'), inplace=True)
    field_counts = field_counts / field_counts.apply(sum)
    field_counts.to_csv('eng_ai.csv')


def extract_top_field(df, id_to_name):
    df = df.copy()
    # Restrict to L0 fields
    df['fields'] = df['fields'].apply(
        lambda x: {id_to_name[field['id']]: field['score'] for field in x if field['id'] in id_to_name})
    # Extract top-scoring field
    df['top_field'] = df['fields'].apply(lambda x: sorted(x.items(), key=lambda x: x[1], reverse=True)[0][0])
    return df


def summarize_top_field(df):
    # Count publication top fields by the scholar category of their venue
    return df.groupby(['scholar_cat', 'top_field'])['id'].agg('count').sort_values(ascending=False)


if __name__ == '__main__':
    main()