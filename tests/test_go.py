import csv
import json
import os

import numpy as np
import pandas as pd

try:
    os.chdir('tests')
except FileNotFoundError:
    pass


def read_go_output():
    go_output_path = "/home/james/.go/src/corpus/output.tsv"
    output = {}
    with open(go_output_path, 'rt') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            if 'score' in row:
                output[(row['merged_id'], row['score'])] = row
            else:
                output[row['merged_id']] = row
    return output


def read_py_scores():
    py_output_path = '../scripts/en_scores.jsonl'
    output = {}
    with open(py_output_path, 'rt') as f:
        for line in f:
            record = json.loads(line)
            output[record['merged_id']] = record
    return output


def go_to_pandas(method):
    go = read_go_output()
    go_df = pd.DataFrame.from_dict(go, orient='index')
    go_df.reset_index(inplace=True)
    go_df = go_df.query(f'level_1 == "{method}"')
    go_df.drop(columns=['level_0', 'level_1', 'score'], inplace=True)
    go_df.set_index('merged_id', inplace=True)
    go_df = go_df.sort_index()
    go_df = go_df.astype(np.float64)
    return go_df


def py_to_pandas(method):
    py = read_py_scores()
    py_long = {}
    for doc, embeds in py.items():
        py_long[(doc, method)] = embeds[method]
    py_df = pd.DataFrame.from_dict(py_long, orient='index')
    py_df.reset_index(inplace=True)
    py_df.drop(columns=['level_1'], inplace=True)
    py_df.set_index('level_0', inplace=True)
    py_df = py_df.sort_index()
    return py_df


def take_diffs(a, b):
    assert (a.index == b.index).all()
    assert set(b.columns) == set(a.columns)
    diffs = []
    for col in b.columns:
        diffs.append(b[col] - a[col])
    diffs = pd.concat(diffs, axis=1)
    diffs = diffs.applymap(abs)
    return diffs


def test_scores():
    py_ft = py_to_pandas('fasttext')
    go_ft = go_to_pandas("fastText")
    diff_ft = take_diffs(go_ft, py_ft)
    diff_ft.mean().mean()

    py_ent = py_to_pandas('entity')
    go_ent = go_to_pandas("entity")
    diff_ent = take_diffs(go_ent, py_ent)
    diff_ent.mean().mean()

    py_tfidf = py_to_pandas('tfidf')
    go_tfidf = go_to_pandas("tfidf")
    diff_tfidf = take_diffs(go_tfidf, py_tfidf)
    diff_tfidf.mean().mean()
