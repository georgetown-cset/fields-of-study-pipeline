"""
Evaluate v1 and v2 scoring against manual annotation.

We have continuous scores in 0-1 and discrete labels, so for P/R/F scoring we have to dichotomize the scores. Consider
for example a doc scored {"CS": 0.6, "Engineering": 0.5, ...} and our manual label is "CS".

We can evaluate only the *top* field label using the familiar P/R/F1 metrics. We often use top fields in analysis, so
this is informative.

But we don't want to evaluate only the top field. We'd like the other fields to appear in some reasonable order, to
the extent possible. (It's unclear what ranking the scores for totally unrelated fields should have--we can't really say
whether some CS article is closer to / farther from Art or Philosophy, and we don't care what their rank order is, so
long as the relevant fields appear higher.)

In the first annotation pass, I did multilabel labeling ... selecting all that seemed appropriate. This matches our
intuition for how to assign fields to docs, but not how we score them. Because we don't have any (haven't learned any)
threshold for dichotomizing cosine similarities into relevant / not relevant. Calculating P/R/F for all labels
simultaneously doesn't really make sense...

Most intuitively, we have a multilabel problem. But the solution we have is for retrieval / recommendation. Given a
document, which fields are most relevant? And given a field, which documents are most relevant. A reasonable way to
represent this is with top-k accuracy or precision @ n. This answers the questions, "how often is the true top field
our top-scoring field?" and then "OK, how often is the true top field one of the top-two-scoring fields?" and so on.
So we want to do both: P/R/F for the top score; and precision @ n for n in [1,3].

In any case, we want manual labeling to provide not only which fields are relevant, but their rank order.
"""
import json

import numpy as np
import pandas as pd
from sklearn.metrics import classification_report, label_ranking_average_precision_score, dcg_score

from fos.settings import ASSETS_DIR

STEM = ("Biology", "Chemistry", "Computer science", "Engineering", "Environmental science", "Geology",
        "Materials science", "Mathematics", "Physics")


def main():
    top_field_report, top_k_report, dcg, lrap = summarize()
    print(top_field_report.to_string(float_format='%.2f', index=False))
    print(top_k_report.to_string(float_format='%.2f', index=False))
    print(dcg.to_string(float_format='%.2f', index=False))
    print(lrap.to_string(float_format='%.2f', index=False))


def summarize(keep_fields=STEM):
    # The annotation data uses field display names; the scoring uses IDs; so load a mapping of ids to names
    meta = pd.read_pickle(ASSETS_DIR / "fields/fos.pkl.gz")
    meta.index = meta.index.astype(int)

    # So far we've only annotated L0 labels, and we're only evaluating L0 scores
    id_to_name = meta.query("level == 0")["display_name"].to_dict()
    if keep_fields is not None:
        id_to_name = {k: v for k, v in id_to_name.items() if v in keep_fields}
    names = list(sorted(id_to_name.values()))

    # Load the manual annotations
    true = pd.read_csv("en_l0_fields_annotations.csv")
    true.set_index("merged_id", inplace=True)
    # Restrict to L0s selected above
    true = true[id_to_name.values()].to_dict(orient='index')
    # We'll rely below on consistent sort by display name
    true = dict(sorted(true.items()))

    # Load the V1 predictions
    v1_pred = read_predictions("en_v1_scores.jsonl", id_to_name)
    # We have continuous scores in 0-1 and discrete labels ... see above for discussion
    binary_v1_pred = {k: dichotomize(v, top_k=1) for k, v in v1_pred.items()}
    v1_report = classification_report(to_arrays(true), to_arrays(binary_v1_pred), target_names=names, output_dict=True)

    v2_pred = read_predictions("en_v2_scores.jsonl", id_to_name)
    binary_v2_pred = {k: dichotomize(v, top_k=1) for k, v in v2_pred.items()}
    v2_report = classification_report(to_arrays(true), to_arrays(binary_v2_pred), target_names=names, output_dict=True)

    top_k_report = pd.DataFrame.from_dict({
        'k': range(5),
        'v1': top_k_accuracy(true, v1_pred, top_k=5, proportion=True).values(),
        'v2': top_k_accuracy(true, v2_pred, top_k=5, proportion=True).values(),
    })

    top_field_report = pd.DataFrame.from_dict({
        'label': [k for k in v1_report.keys()],
        f'v1_top_f1': [x['f1-score'] for x in v1_report.values()],
        f'v2_top_f1': [x['f1-score'] for x in v2_report.values()],
        'support': [x['support'] for x in v2_report.values()],
    })

    # https://scikit-learn.org/stable/modules/model_evaluation.html#multilabel-ranking-metrics
    v1_array = pred_to_array(v1_pred)
    v2_array = pred_to_array(v2_pred)
    true_array = true_to_array(true)
    v1_rank_array = pred_to_array(v1_pred, ranks=True)
    v2_rank_array = pred_to_array(v2_pred, ranks=True)

    dcg = []
    for k in range(1, 6):
        dcg.append({
            'k': k,
            'v1': dcg_score(true_array, v1_array, k=k),
            'v2': dcg_score(true_array, v2_array, k=k)
        })
    dcg = pd.DataFrame(dcg)

    lrap = {
        'v1': [label_ranking_average_precision_score(true_array, v1_array)],
        'v2': [label_ranking_average_precision_score(true_array, v2_array)],
    }
    lrap = pd.DataFrame(lrap)

    return top_field_report, top_k_report, dcg, lrap


def top_k_accuracy(true_values, pred_values, top_k=5, proportion=True):
    true_values = list(true_values.values())
    pred_values = list(pred_values.values())
    assert len(true_values) == len(pred_values)
    n = 0
    correct = {i: 0 for i in range(top_k)}
    for true, pred in zip(true_values, pred_values):
        true_fields = {k for k, v in true.items() if v}
        if not len(true_fields):
            # If we've restricted the labels for evalutaion to STEM fields, discard the docs with no positive labels
            continue
        # Ensure that fields are in order of score descending
        pred = [k for k, v in sorted(pred.items(), key=lambda x: x[1], reverse=True)]
        # This is a hack for having annotated multilabel -- if there are 2 true labels, we're taking them
        # interchangeably. We look at the top-scoring label, and check whether it's among the true labels. If so, we
        # count that as accurate @ 1. If not, we check the 2nd-scoring label. If among the true labels, that's accurate
        # @ 2
        for i in range(top_k):
            if pred[i] in true_fields:
                correct[i] += 1
                break
        n += 1
    # The above gave us counts for how often the top-scoring field is correct; if not, how often the 2nd-ranked field
    # label is correct ... and so on to the 5th-ranked field. Taking a cumulative sum over these counts gives us how
    # often a correct label (*the* true label if we manually chose a single label, or one of the correct labels if we
    # chose more than one ... in practice it's mostly 1 label; some 2 labels; rarely 3 labels) is the top label; among
    # the top 2 labels; among the top 3 labels ...
    for i in reversed(range(top_k)):
        for j in range(i):
            correct[i] += correct[j]

    if proportion:
        return {k: x / n for k, x in correct.items()}
    return correct


def read_predictions(path, id_to_name):
    pred = {}
    with open(path, "rt") as infile:
        for line in infile:
            doc = json.loads(line)
            l0_scores = {}
            for field in doc["fields"]:
                try:
                    l0_scores[id_to_name[field["id"]]] = field["score"]
                except KeyError:
                    # field isn't an L0
                    continue
            assert len(l0_scores) == len(id_to_name)
            pred[doc["id"]] = l0_scores
    pred = dict(sorted(pred.items()))
    return pred


def to_arrays(doc_scores):
    # Structure scores for sklearn.metrics
    arrays = []
    for doc_id, scores in doc_scores.items():
        arrays.append([v for k, v in sorted(scores.items())])
    return arrays


def dichotomize(scores, top_k=1):
    """Dichotomize an array of continuous field scores such that the top_k fields are true and the rest false."""
    output = {}
    ordered_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    for i, (label, score) in enumerate(ordered_scores, 1):
        output[label] = i <= top_k
    return output


def true_to_array(true):
    arrays = []
    for row in true.values():
        sorted_row = dict(sorted(row.items()))
        arrays.append(np.array(list(sorted_row.values())).astype(int))
    return np.array(arrays)


def pred_to_array(pred, ranks=False):
    arrays = []
    for row in pred.values():
        sorted_row = dict(sorted(row.items()))
        row_array = np.array(list(sorted_row.values()))
        if ranks:
            arrays.append(np.array(list(reversed(row_array.argsort()))) + 1)
        else:
            arrays.append(row_array)
    return np.array(arrays)


if __name__ == '__main__':
    main()
