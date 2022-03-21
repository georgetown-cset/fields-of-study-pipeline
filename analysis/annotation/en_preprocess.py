"""
Preprocess the annotated sample of docs for field scoring.
"""
import json

import pandas as pd

from fos.util import preprocess


def main():
    docs = preprocess_annotated_texts()
    with open("en_preprocessed_texts.jsonl", "wt") as f:
        for doc in docs:
            f.write(json.dumps(doc) + "\n")


def preprocess_annotated_texts():
    texts = []
    df = pd.read_csv("en_l0_fields_annotations.csv")
    for _, doc in df.iterrows():
        texts.append({
            "merged_id": doc["merged_id"],
            "text": preprocess_text(doc)
        })
    return texts


def preprocess_text(record):
    text = ""
    if not pd.isnull(record["title"]):
        text += record["title"] + " "
    if not pd.isnull(record["abstract"]):
        text += record["abstract"]
    return preprocess(text, "en")


if __name__ == '__main__':
    main()
