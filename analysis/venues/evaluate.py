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
    meta = pd.read_pickle("fos.pkl.gz")
    meta.index = meta.index.astype(int)
    id_to_name = meta.query("level == 0")["display_name"].to_dict()

    venues = pd.read_json('ai_venue_text.jsonl', lines=True)
    del venues['text']

    mag = pd.read_json('ai_venue_text_mag_scores.jsonl', lines=True)
    mag = pd.merge(mag, venues, on='id', how='inner')
    mag = restrict(mag, id_to_name)
    mag = extract_top_field(mag, 1)
    mag_k1 = summarize_top_field(mag)
    mag = extract_top_field(mag, 2)
    mag_k2 = summarize_top_field(mag)

    cset = pd.read_json('ai_venue_text_cset_scores.jsonl', lines=True)
    cset = pd.merge(cset, venues, on='id', how='inner')
    cset = restrict(cset, id_to_name)
    cset = extract_top_field(cset, 1)
    cset_k1 = summarize_top_field(cset)
    cset = extract_top_field(cset, 2)
    cset_k2 = summarize_top_field(cset)

    comparison_k1 = merge(mag_k1, cset_k1)
    comparison_k1.round(4).to_csv('eng_ai_k1.csv')

    comparison_k2 = merge(mag_k2, cset_k2)
    comparison_k2.round(4).to_csv('eng_ai_k2.csv')

    venues[['scholar_cat', 'journal_name']].drop_duplicates().to_csv('eng_ai_venues.csv', index=False)


def merge(mag_fields, cset_fields):
    field_counts = pd.merge(mag_fields, cset_fields, suffixes=('_mag', '_cset'), left_index=True, right_index=True)
    field_counts.rename(columns=lambda x: x.replace('id_', 'n_'), inplace=True)
    field_counts = field_counts / field_counts.apply(sum)
    field_counts = field_counts.sort_values('n_mag', ascending=False)
    return field_counts


def restrict(df, id_to_name):
    # Restrict to docs with scores
    df = df.loc[df['fields'].apply(len) > 0].copy()
    # Restrict to L0 fields
    df['fields'] = df['fields'].apply(
        lambda x: {id_to_name[field['id']]: field['score'] for field in x if field['id'] in id_to_name})
    return df


def extract_top_field(df, k=1):
    # Extract top-scoring field
    df[f'top_field_{k}'] = df['fields'].apply(lambda x: sorted(x.items(), key=lambda x: x[1], reverse=True)[k - 1][0])
    return df


def summarize_top_field(df):
    # Count publication top fields by the scholar category of their venue
    field_cols = [col for col in df.columns if col.startswith('top_field')]
    return df.groupby(['scholar_cat'] + field_cols)['id'].agg('count').sort_values(ascending=False)


if __name__ == '__main__':
    main()
