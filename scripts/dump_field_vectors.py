import csv
import json
from collections import defaultdict
from itertools import zip_longest

from fos.settings import EN_FIELD_TFIDF_PATH
from fos.vectors import load_field_tfidf


def main():
    field_tfidf = load_field_tfidf("en")
    index = field_tfidf.index.tocoo()
    with open(EN_FIELD_TFIDF_PATH.with_suffix('.csv'), 'wt') as f:
        writer = csv.writer(f)
        for i, j, x in zip_longest(index.row, index.col, index.data):
            writer.writerow([i, j, x])

    fields = defaultdict(dict)
    for (field_id, token_id), tfidf in index.items():
        fields[int(field_id)][int(token_id)] = float(tfidf)
    with open(EN_FIELD_TFIDF_PATH.with_suffix('.json'), 'wt') as f:
        json.dump(fields, f)


if __name__ == '__main__':
    main()
