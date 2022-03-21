"""
Prepare a ZH sample for annotation.

Specifically,
- Draw the ZH sample for annotation from BQ;
- Translate the text to EN via the translation API;
- Preprocess the text for local scoring (easier to re-run locally than retrieve the latest scores);
- Preprocess the text for annotation via Prodigy.

Output `zh_preprocessed.json` is Prodigy-ready, but also has a preprocessed `text` field for scoring.
"""
import gzip
import json
from pathlib import Path

from fos.corpus import Translator
from fos.gcp import write_query, download_query
from fos.util import preprocess as preprocess_text


def main():
    download()
    docs = list(preprocess())
    # Not all docs have titles and abstracts, so we need an ID=>doc mapping for book-keeping
    docs = {doc["merged_id"]: doc for doc in docs}
    # Update docs in place to
    translate_text(docs, "title")
    translate_text(docs, "abstract")
    with open("zh_preprocessed.json", "wt") as f:
        json.dump(list(docs.values()), f, indent=2)


def translate_text(docs, field, batch_size=50):
    """Send doc text to the translation API in batches."""
    texts = {doc_id: doc.get(field) for doc_id, doc in docs.items() if doc.get(field)}
    translator = Translator()
    # We rely on these coming back in order
    result = translator.translate_batch(texts.values(), batch_size)
    # We chunked them up and have to flatten them
    translations = [title for batch in result for title in batch]
    assert len(texts) == len(translations)
    # Add e.g. "en_title"
    for doc_id, translation in zip(texts.keys(), translations):
        docs[doc_id][f'en_{field}'] = translation


def preprocess():
    """Preprocess the ZH BQ extract."""
    # We probably have a single file with a name like "zh-sample-2022-03-20-000000000.jsonl.gz"
    paths = list(Path(".").glob("zh-sample-2022-03-20*.jsonl.gz"))
    assert len(paths)
    for path in paths:
        with gzip.open(path) as f:
            for line in f:
                doc = json.loads(line)
                # Preprocess the text for local scoring
                doc["text"] = preprocess_text(concat_text(doc), lang="zh")
                yield doc


def concat_text(record):
    """Combine whatever title and abstract text is available into a single string."""
    text = ""
    if "title" in record and record["title"]:
        text += record["title"] + " "
    if "abstract" in record and record["abstract"]:
        text += record["abstract"]
    return text


def download():
    # Create the sampling frame table
    sampling_frame_table = "field_model_replication.zh_annotation_sampling_frame_2022_03_20"
    # Assume we're local in analysis/annotation
    write_query(Path("zh_sampling_frame.sql"),
                destination=sampling_frame_table,
                clobber=True)
    # Draw a small sample from the sampling frame
    sample_table = "field_model_replication.zh_annotation_sample_2022_03_20"
    download_query(Path("zh_sample.sql"),
                   sample_table,
                   "fields-of-study-model",
                   "zh-sample-2022-03-20/zh-sample-2022-03-20",
                   Path.cwd(),
                   clobber=True)


if __name__ == '__main__':
    main()
