## Overview

## Workflow

### 0. `read_field_meta.py`

*Note*: this file has not been updated or rerun between the old version of fields of study and the new version, unlike all the following files. (We don't need it.)

First we identify the fields we'll be looking for.

We read MAG's field metadata and write out a TSV that contains the key identifiers from MAG (integer field ID, display 
and normalized name) the level of each field, and our best guess as to its corresponding Wikipedia page. 
The field attributes table in MAG included English Wikipedia URLs for many but not all fields.
They're more available at higher levels (0-2).
Some fields have Wikipedia pages that aren't given in the field attributes table.
So if that table doesn't identify a page for a field, we try finding a page using the field name.

Inputs: final field metadata from MAG in `data/fos.pkl.gz` and `data/fos_attr.pkl.gz`. 
These correspond with the `FieldsOfStudy` and `FieldOfStudyAttributes` tables in MAG, respectively.
They're pickled dataframes created using code in the `fields-of-study-model` repo.

Process: 
```shell
python read_field_meta.py
```

For now, we restrict to fields in levels 0-1 to cut down on the time we'll later spend hitting the Wikipedia API.

Output: `data/fields.tsv`, which looks like

| id         | level | display_name | normalized_name | wiki_title   |
| ---------- | ----- | ------------ | --------------- | ------------ |
| 2522767166 |     1 | Data science | data science    | Data science |
| .......... |     . | ............ | ............    | ............ |
| .......... |     . | ............ | ............    | ............ |

### 1. `edit_field_meta.py`

*Note*: This is a new file for the new version of fields of study, added to insert in new custom fields rather than relying on the ones in MAG.

Here, we modify our fields list to include our custom fields for levels 2 and 3, and replace any fields in lower levels that we're using instead of the original MAG fields. We also modify our field hierarchy, which we'll use later in the process when we're actually sharing our final fields, but which we want to make sure is accurate now.

The important things are to make sure that we correctly replace fields in level 0 or 1 that we're modifying from MAG, and that we include all needed information about our fields for later analysis.

Process:

First, place your custom levels2and3.tsv document in wiki/data.

```shell
python edit_field_meta.py
```

### 2. `fetch_page_titles.py`

Next we find Wiki page titles for as many fields as possible.

Given the English Wikipedia page title for a field (if known) or otherwise the field name in English, we hit the 
Mediawiki API asking for metadata on any such page in English Wikipedia. Specifically, we request its `langlinks`
property, which describes corresponding pages in other languages/Wikipedias. (In the browser, these are the language 
links on the bottom of the left-hand nav.)

In the case we don't find the actual page, we instead fall back to searching Wikipedia for this term (in a second API request), in case
   there's a near match. We write that field-to-page mapping to `data/search.tsv` for manual review.

Input: field metadata from previous step in `data/all_fields.tsv`.

Process:
```shell
python fetch_page_titles.py
```

Then review `data/search.tsv`, which looks like this (but without the header):

| search term | top page result           |
|------------|----------------------------|
| index.html | Web server directory index |
| β diketone | 2-Iodoxybenzoic acid       |
| α helices  | Alpha helix                |

Here the first and third searches look successful, but the second wasn't. 
Review of the [2-Iodoxybenzoic acid](https://en.wikipedia.org/wiki/2-Iodoxybenzoic_acid#Oxidation_of_%CE%B2-hydroxyketones_to_%CE%B2-diketones)
page shows that it's just a reagent used with β diketone, our field and concept of interest.
We add the successful searches to `data/manual_page_titles.tsv`.

Next, review `data/not_found.txt`. 
It's probably empty, but fields appear here if looking for a page with the same name as our field was unsuccessful, and 
searching for the name of our field returned no results.

If any new successful searches were added to `data/manual_page_titles.tsv`, re-run to include them in results.

Output: `data/field_pages.json`, `data/search.tsv`, `data/not_found.txt`.

### 3. `describe_page_titles.py`

We run this script to summarize and validate the results of the previous step.

```shell
python describe_page_titles.py
```

Example output:
```shell
# 0 pages missing from output:
# Series([], Name: wiki_title_1, dtype: object)
# 
# Coverage by level (n second titles, n primary EN titles):
#       en_title_2
# level           
# 0        (0, 19)
# 1       (0, 278)
# 2       (3, 104)
# 3       (1, 704)
```

Ideally all the fields in the input would also appear in the output, but this may not be true.

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

### 7. `embed_field_text.py`

We next embed the text for each field.
The paths below assume the `field-of-study-pipelines` repo can be found in the parent of this project's directory. 

English:

```shell
python embed_field_text.py \
  ../field-of-study-pipelines/assets/scientific-lit-embeddings/english/fasttext/en_merged_model_120221.bin \
  ../field-of-study-pipelines/assets/scientific-lit-embeddings/english/tfidfs/tfidf_model_en_merged_sample.pkl \
  ../field-of-study-pipelines/assets/scientific-lit-embeddings/english/tfidfs/id2word_dict_en_merged_sample.txt \
  --lang=en
```

*Note*: This program currently have to be run from one level higher than the previous programs, e.g. not from the wiki directory, as follows:

`python -m wiki.embed_field_text`

This is because it relies on the `fos` module.

Outputs: `en_field_fasttext.npz` and `en_field_tfidf.npz`.