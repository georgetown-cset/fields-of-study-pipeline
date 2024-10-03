## Overview

This repo contains materials for the fields of study project (as described in ``Multi-Label Field Classification for Scientific Documents using Expert and
Crowd-sourced Knowledge''). This involves the following:

1. Our starting point is our merged corpus of publications, specifically its English-language text. We use it to learn _FastText_ and _tf-idf
   word vectors_.

2. The second fundamental input is a taxonomy that defines fields of study, in a hierarchy of broad areas like
   "computer science" and more granular subfields like "machine learning". We derived the top level of this taxonomy from the taxonomy previously used by MAG,
   and create the lower layers ourselves (as described in the paper). For
   current purposes it's static. We call this the _field taxonomy_.

4. For each field in the taxonomy, we have various associated text extracted from Wikipedia (pages and their references).
   Using this _field content_ and the word vectors learned from the merged corpus, we create embeddings for each
   field. We refer to these as FastText and tf-idf _field embeddings_.

5. We then identify in the field content every mention of another field. (For instance, the "computer science" content
   mentions "artificial intelligence," "machine learning," and many other fields.) The averages of the FastText field 
   embeddings for these mentioned fields are the _entity embeddings_ for each field.

6. Next, for each English publication in the merged corpus we create _publication embeddings_. Specifically, for each
   publication a _FastText embedding_, _tf-idf embedding_, and _FastText field mention embedding_ (as immediately above,
   but for fields mentioned in the publication text).

7. Lastly, scoring: we compute the cosine similarities of the embeddings for publications and fields. This yields up to
   three cosine similarity (FastText, tf-idf, and mention FastText) for a publication-field pair. We average them to get
   a publication's field score.

## Setup

Clone:

```shell
git clone --recurse-submodules https://github.com/georgetown-cset/fields-of-study-pipeline.git
```

On Linux with Python 3.8.10 via Miniconda, pip-install the requirements file:

```shell
cd fields-of-study-pipeline
~/miniconda3/bin/python3 -m venv venv
source venv/bin/activate

sudo apt-get install build-essential -y
pip install -r requirements.txt
```

Some assets are large, so we're using [dvc](https://dvc.org/doc/start). 
GitHub is responsible for tracking the asset metadata in `.dvc` files and DVC stores the assets themselves in GCS.
Retrieve them with `dvc pull`.

```shell
dvc pull
cd assets/scientific-lit-embeddings/ && dvc pull && cd ../..
```

## GCP

We have an [instance](https://console.cloud.google.com/compute/instancesDetail/zones/us-east1-c/instances/fields?project=gcp-cset-projects)
named `fields` in us-east1-c. It's set up as above.

DVC is backed by storage in the `gs://fields-of-study-model` bucket. 
When retrieving the merged corpus (`fos/corpus.py`) we use the 
[`field_model_replication`](https://console.cloud.google.com/bigquery?project=gcp-cset-projects&p=gcp-cset-projects&d=field_model_replication&page=dataset)
BQ dataset and the `gs://fields-of-study` bucket.

## Pipeline

Retrieve English text in our merged corpus:

```shell
# writes 'assets/corpus/{lang}_corpus-*.jsonl.gz'
PYTHONPATH=. python scripts/download_corpus.py en
```

Embed the publication text:

```shell
# reads 'assets/corpus/{lang}_corpus-*.jsonl.gz'
# writes 'assets/corpus/{lang}_embeddings.jsonl'
PYTHONPATH=. python scripts/embed_corpus.py en
```

Calculate field scores from the publication embeddings:

```shell
# reads 'assets/corpus/{lang}_embeddings.jsonl'
# writes 'assets/corpus/{lang}_scores.tsv'
PYTHONPATH=. python scripts/score_embeddings.py en
```

Alternatively, embed + score without writing the publication embeddings to the disk:

```shell
# reads 'assets/corpus/{lang}_corpus-*.jsonl.gz'
# writes 'assets/corpus/{lang}_scores.jsonl'
PYTHONPATH=. python scripts/score_corpus.py en
```

## Project workflow

### 1. Merged corpus text and word vectors

We start by retrieving English in the merged corpus.

```shell
PYTHONPATH=. python scripts/download_corpus.py en
```

We learned English FastText and tf-idf vectors from these corpora. Documentation for this is
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

### 4. Entity embeddings

We identify in the field content every mention of another field. For instance, the "computer science" content mentions 
"artificial intelligence," "machine learning," etc. We average over the FastText embeddings for mentioned fields to
generate FastText _entity embeddings_. This is documented in the `wiki-field-text` repo.

Outputs (annually):

- FastText entity embeddings: `assets/{en,zh}_field_mention_fasttext.bin`

### 5. Publication embedding

We embed each English publication in the preprocessed corpora (1) using the FastText and tf-idf vectors (2), and by
averaging the entity embeddings (3) for each field mentioned in the publication text.

```shell
PYTHONPATH=. python scripts/embed_corpus.py en
```

Outputs (~weekly):

- Publication embeddings: `assets/corpus/{lang}_embeddings.jsonl`

### 6. Field scoring

For publication-field pairs, we take the cosine similarity of the publication and field embeddings, and then average
over these cosine similarities yielding field scores.

```shell
PYTHONPATH=. python scripts/score_embeddings.py en
```

Outputs (~weekly):

- Publication field scores: `assets/corpus/{lang}_scores.jsonl`

---

### Airflow deployment

To add new queries to the sequence that is run after scores are ingested into BQ, add the query to the `sql` directory
and put the filename in `query_sequence.txt` in the position the query should be run. You can reference the
staging and production datasets in your query using `{{staging_dataset}}` and `{{production_dataset}}`. Because
the production dataset contains old data until the sequence of queries finishes running, you normally want to
reference the staging dataset.

To update the artifacts used by airflow, run `bash push_to_airflow.sh`.

To view the dag, visit [this link](https://sc8c690a9f43753bep-tp.appspot.com/graph?dag_id=new_fields_of_study). To
trigger a run on only new or modified data, trigger the dag without configuration parameters. To rerun on all data, 
trigger the dag with the configuration `{"rerun": true}`.
