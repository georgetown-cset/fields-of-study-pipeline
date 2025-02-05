## Overview

## Workflow

### 4. `fetch_page_content.py`

Armed with the Wiki page titles from the previous step, we retrieve the content of these pages.
This requires a lot of API calls and may periodically fail.
For persistence, we use SQLite.
Our script iterates over the pages ID'd in the previous step, and if we don't already have their content in the DB,
requests it from the API.

Inputs: `data/field_pages.json`.

Process:
```shell
python fetch_page_content.py
```

Outputs: `data/wiki.db`.

### 5. `extract_text.py`

Next we get the text content for each field ready for embedding.

Inputs: `data/wiki.db`.

### 6. `fetch_reference_text.py`

Here, using the text content extracted, we find the reference text from each field and add it in.

Inputs: `data/wiki.db`.

*Note*: This program currently have to be run from one level higher than the previous programs, e.g. not from the wiki directory, as follows:

`python -m wiki.fetch_reference_text`

This is because it relies on the `fos` module.

### 7. `describe_text_coverage.py`

We use this program to ensure our text coverage is good and consistent. Example output is below:

```There are 1076 records in the refs table, and of those, 941 have at least one reference.```

### 8. `embed_field_text.py`

We next embed the text for each field.
The paths below assume the `field-of-study-pipelines` repo can be found in the parent of this project's directory. 

English:

```shell
python embed_field_text.py 
```

*Note*: This program imports the `fos` module, so from the `./wiki` directory, invoke it like 

```shell
PYTHONPATH=.. python embed_field_text.py
```

### 9. `embed_entities.py`

This script uses outputs from the previous step to create entity-mention embeddings using the field text and the output from the previous step. 
Entity-mention embeddings are created by averaging the embeddings of the fields mentioned in the text.

We have two outputs: (1) an entity matcher to efficiently find field mentions in publication text, yielding
the corresponding field vectors; and (2) a matrix of entity vectors for fields (via `gensim.similarities.docsim.MatrixSimilarity`), for scoring purposes: comparison of entity-based publication embeddings against entity-based field embeddings.
