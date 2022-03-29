import os

import numpy as np
import pandas as pd

from fos.util import read_go_output, read_output, run_go

try:
    os.chdir('tests')
except FileNotFoundError:
    pass


def test_go_scores_fixture(en_go_scores):
    assert isinstance(en_go_scores, dict)


def test_en_scores_fixture(en_scores):
    assert isinstance(en_scores, dict)


def test_py_vs_go_keys(en_scores, en_go_scores):
    assert set(en_scores.keys()) == set(en_go_scores.keys())


def test_bad_asset_path(preprocessed_text_file):
    stdout = run_go(preprocessed_text_file, '/dev/null', "--assets ~/")
    print(stdout)


def test_py_vs_go(en_scores, en_go_scores, meta):
    eps = .05
    for k in en_scores.keys():
        py = en_scores[k]
        go = en_go_scores[k]
        agreement = []
        for field in py.keys():
            if abs(py[field] - go[field]) > eps:
                print(field, meta.loc[str(field), 'display_name'], round(py[field], 4), go[field])
                agreement.append(0)
            else:
                agreement.append(1)
        print(sum(agreement), len(agreement))
        assert sum(agreement) == len(agreement)


def go_to_pandas(method):
    go = read_go_output("/home/james/.go/src/corpus/output.tsv")
    go_df = pd.DataFrame.from_dict(go, orient='index')
    go_df.reset_index(inplace=True)
    go_df = go_df.query(f'level_1 == "{method}"')
    go_df.drop(columns=['level_0', 'level_1', 'score'], inplace=True)
    go_df.set_index('merged_id', inplace=True)
    go_df = go_df.sort_index()
    go_df = go_df.astype(np.float64)
    return go_df


def py_to_pandas(method):
    py = read_output('../scripts/en_scores.jsonl')
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
    if len(py_ft.index) != len(go_ft.index):
        shorter_index_length = min(len(py_ft.index), len(go_ft.index))
        py_ft = py_ft.iloc[:shorter_index_length]
        go_ft = go_ft.iloc[:shorter_index_length]
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
