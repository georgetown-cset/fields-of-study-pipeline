from fos.util import run


def test_run():
    r = run("go/fields")
    assert r.returncode == 0
    assert 'show help (default: false)' in r.stdout


def test_go_scores_fixture(en_go_scores):
    assert isinstance(en_go_scores, dict)


def test_en_scores_fixture(en_scores):
    assert isinstance(en_scores, dict)


def test_py_vs_go_keys(en_scores, en_go_scores):
    assert set(en_scores.keys()) == set(en_go_scores.keys())


def test_bad_asset_path(preprocessed_jsonl_path):
    result = run(f"go/fields score -i {preprocessed_jsonl_path} -o /dev/null --assets /__", shell=True)
    assert result.returncode == 1
    assert 'directory not found' in result.stderr


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
