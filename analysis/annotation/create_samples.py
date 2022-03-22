"""
Create samples for manual annotation.
"""
import json
from pathlib import Path

import pandas as pd

from fos.util import preprocess


def main():
    cs_sample = draw_sample("en_cs_sampling_frame", "en", n=800)
    not_cs_sample = draw_sample("en_non_cs_sampling_frame", "en", n=200)
    sample = pd.concat([cs_sample, not_cs_sample]).sample(frac=1)
    sample["text"] = sample.apply(lambda row: preprocess_text(row, lang="en"), axis=1)
    sample.reset_index(inplace=True)
    with open('top_cs_sample.json', 'wt') as f:
        json.dump(sample.to_dict(orient='records'), f, indent=2)



def draw_sample(frame_table, lang="en", n=1_000):
    sql = load_sample_sql(frame_table, lang)
    sample = pd.read_gbq(sql, project_id='gcp-cset-projects', index_col='merged_id')
    sample = sample.sample(n=n, random_state=20220321)
    print(f'Drew {sample.shape[0]} papers')
    return sample


def load_sample_sql(frame_table, lang="en"):
    sql = Path("sample_template.sql").read_text()
    sql = sql.replace("{{frame_table}}", frame_table)
    sql = sql.replace("{{lang}}", lang)
    return sql


def preprocess_text(record, lang="en"):
    text = ""
    if not pd.isnull(record["title"]):
        text += record["title"] + " "
    if not pd.isnull(record["abstract"]):
        text += record["abstract"]
    return preprocess(text, lang)


def test_sample_is_deterministic():
    # confirm deterministic
    cs_sample = draw_sample("en_cs_sampling_frame", "en", n=500)
    repeated_cs_sample = draw_sample("en_cs_sampling_frame", "en", n=500)
    assert cs_sample.shape[0] == 500
    assert cs_sample.index[0].startswith('carticle')
    assert set(cs_sample.index) == set(repeated_cs_sample.index)


def test_template():
    sql = load_sample_sql("foo", "bar")
    assert 'field_model_replication.foo' in sql
    assert "result_short_code) = 'bar'" in sql


if __name__ == '__main__':
    main()
