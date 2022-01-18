## Overview

This repo contains materials for a replication of MAG's fields of study. This involves the following:

1. Our starting point is the merged corpus, specifically its EN and ZH text. We use it to learn _FastText_ and _tf-idf
   word vectors_.

2. The second fundamental input is a taxonomy that defines fields of study, in a hierarchy of broad areas like
   "computer science" and more granular subfields like "machine learning". We derived this taxonomy from MAG. For
   current purposes it's static. We call this the _field taxonomy_.

3. For each field in the taxonomy, we have various associated text extracted from Wikipedia (pages and their references)
   . Using this _field content_ and the word vectors learned from the merged corpus, we create embeddings for each
   field. We refer to these as _FastText_ and _tf-idf field embeddings_.

4. We then identify in the field content every mention of another field. (For instance, the "computer science" content
   mentions "artificial intelligence," "machine learning," and many other fields.) The averages of the field embeddings
   for these mentioned fields are the _FastText_ and _tf-idf mention embeddings_ for each field.

5. Next, for each EN and ZH publication in the merged corpus we create _publication embeddings_. Specifically, for each
   publication a _FastText embedding_, _tf-idf embedding_, and _FastText field mention embedding_ (as immediately above,
   but for fields mentioned in the publication text).

6. Lastly, scoring: we compute the cosine similarities of the embeddings for publications and fields. This yields up to
   three cosine similarity (FastText, tf-idf, and mention FastText) for a publication-field pair. We average them to get
   a publication's field score.

## Setup

To clone this repo with its submodule, use

```shell
git clone --recurse-submodules https://github.com/georgetown-cset/field-of-study-pipelines.git
```

Embedding assets are in the [scientific-lit-embeddings](https://github.com/georgetown-cset/scientific-lit-embeddings)
submodule (`assets/scientific-lit-embeddings`). They're large, so we're using [dvc](https://dvc.org/doc/start). GitHub
is responsible for tracking the asset metadata in `.dvc` files and DVC stores the assets themselves in GCS.

Ensure ~15 GB of disk is available, then retrieve the assets with

```shell
cd assets/scientific-lit-embeddings
dvc pull
```

On Linux with Python 3.8 via Miniconda, pip-installing the requirements file just works.

```shell
pip install -r requirements.txt
```

## Pipeline

Retrieve EN and ZH text in the merged corpus:

```shell
# writes 'assets/corpus/{lang}_corpus-*.jsonl.gz'
python scripts/download_corpus.py en
python scripts/download_corpus.py zh
```

Embed the publication text:

```shell
# reads 'assets/corpus/{lang}_corpus-*.jsonl.gz'
# writes 'assets/corpus/{lang}_embeddings.jsonl'
python scripts/embed_corpus.py en
python scripts/embed_corpus.py zh
```

Calculate field scores from the publication embeddings:

```shell
# reads 'assets/corpus/{lang}_embeddings.jsonl'
# writes 'assets/corpus/{lang}_scores.tsv'
python scripts/score_embeddings.py en
python scripts/score_embeddings.py zh
```

Alternatively, embed + score without writing the publication embeddings to the disk:

```shell
# reads 'assets/corpus/{lang}_corpus-*.jsonl.gz'
# writes 'assets/corpus/{lang}_scores.jsonl'
python scripts/score_corpus.py en
python scripts/score_corpus.py zh
```

## Project workflow

### 1. Merged corpus text and word vectors

We start by retrieving EN and ZH text in the merged corpus.

```shell
python scripts/download_corpus.py en
python scripts/download_corpus.py zh
```

We learned EN and ZH FastText and tf-idf vectors from these corpora. Documentation for this is
in `assets/scientific-lit-embeddings`.

Outputs (annually):

- FastText vectors: `assets/{en,zh}_merged_fasttext.bin`
- tf-idf vectors and vocab: `assets/{en,zh}_merged_tfidf.bin` and TODO

Outputs (~weekly):

- Preprocessed corpus: `assets/corpus/{lang}_corpus-*.jsonl.gz`

### 2. Field taxonomy

The field taxonomy defines fields of study: their names, the parent/child relations among fields, and other metadata. At
time of writing, we're using a field taxonomy derived from MAG. In the future, we might extend or otherwise update the
taxonomy.

Outputs (static):

- `wiki-field-text/fields.tsv`

### 3. Field content and embeddings

For each field in the field taxonomy, we identified associated text (page content and references) in Wikipedia. This is
documented in the `wiki-field-text` repo. Using this content and the word vectors learned from the merged corpus, we
created embeddings for each field.

Outputs (annually):

- FastText field embeddings: `assets/{en,zh}_field_fasttext.bin`
- tf-idf field embeddings: `assets/{en,zh}_field_tfidf.bin`

### 3. Field mention embeddings

We identify in the field content every mention of another field. For instance, the "computer science" content mentions "
artificial intelligence," "machine learning," etc. We average over the FastText embeddings for mentioned fields to
generate FastText _mention embeddings_ for each field. This is documented in the `wiki-field-text` repo.

Outputs (annually):

- FastText field mention embeddings: `assets/{en,zh}_field_mention_fasttext.bin`

### 4. Publication embedding and scoring

We embed each EN and ZH publication in the preprocessed corpora (1) using the FastText and tf-idf vectors (2), and by
averaging the field mention embeddings (3) for each field mentioned in the publication text.

For publication-field pairs, we take the cosine similarity of the publication and field embeddings, and then average
over these cosine similarities yielding field scores.

Embedding:

```shell
python scripts/embed_corpus.py en
python scripts/embed_corpus.py zh
```

Then scoring:

```shell
python scripts/score_embeddings.py en
python scripts/score_embeddings.py zh
```

Or embedding + scoring together:

```shell
python scripts/score_corpus.py en
python scripts/score_corpus.py zh
```

Outputs (~weekly):

- Publication embeddings: `assets/corpus/{lang}_embeddings.jsonl`
- Publication field scores: `assets/corpus/{lang}_scores.tsv`
