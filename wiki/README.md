## Overview

## Workflow

### 0. `read_field_meta.py`

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

### 1. `fetch_page_titles.py`

Next we find Wiki page titles for as many fields as possible.

Given the English Wikipedia page title for a field (if known) or otherwise the field name in English, we hit the 
Mediawiki API asking for metadata on any such page in English Wikipedia. Specifically, we request its `langlinks`
property, which describes corresponding pages in other languages/Wikipedias. (In the browser, these are the language 
links on the bottom of the left-hand nav.) This maps English-language fields to Chinese-language fields.

There are a few possible outcomes:

1. We look up an English page whose name matches that of a field like "statistical lempel-viz" but it doesn't exist
   (maybe it once did; maybe not). We fall back to searching Wikipedia for this term (in a second API request), in case
   there's a near match. We write that field-to-page mapping to `data/search.tsv` for manual review.
2. We find the desired English page but the `langlinks` property doesn't include a link to a corresponding page on 
   Chinese Wikipedia. We write out just the English page name and ID, along with the MAG metadata, to 
   `data/field_pages.json`.
3. We find the desired English page and a linked Chinese page. We write out each page name and page ID, along with the 
   MAG metadata, to `data/field_pages.json`.

Input: field metadata from previous step in `data/fields.tsv`.

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

### 2. `describe_page_titles.py`

We run this script to summarize and validate the results of the previous step.

```shell
python describe_page_titles.py
```

Example output:
```shell
# 7 pages missing from output:
# id
# 2985900164                Spatial behavior
# 2984324743             Interactive control
# 2987823439             Differential method
# 2986150514                     Self repair
# 2985904603            Information networks
# 2987955292    Sudden infant death syndrome
# 2987264701                  Peer influence
# Name: wiki_title, dtype: object
# 
# Coverage by level (n ZH, n EN):
#               zh_title
# level                 
# 0             (19, 19)
# 1           (277, 292)
# 2      (23123, 137289)
```

Ideally all the fields in the input would also appear in the output, but this may not be true.
We also expect far less coverage of fields in Chinese Wikipedia than in English Wikipedia, but want to check how much.
In the above output, it looks good in levels 0-1 and much worse at level 2. 
The distribution of coverage could matter---conceivably the pages that don't appear in Chinese Wikipedia correspond with 
level-2 fields we aren't interested in (e.g., non-STEM).

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

### 5. `write_field_text.py`

Next we get the text content for each field ready for embedding.

Inputs: `data/wiki.db`.
Outputs: `data/field_content.json` and `data/field_text.tsv`.

### 5. `embed_field_text.py`

Last step! We embed the text for each field.
The paths below assume the `field-of-study-pipelines` repo can be found in the parent of this project's directory. 

English:

```shell
python embed_field_text.py \
  ../field-of-study-pipelines/assets/scientific-lit-embeddings/english/fasttext/en_merged_model_120221.bin \
  ../field-of-study-pipelines/assets/scientific-lit-embeddings/english/tfidfs/tfidf_model_en_merged_sample.pkl \
  ../field-of-study-pipelines/assets/scientific-lit-embeddings/english/tfidfs/id2word_dict_en_merged_sample.txt \
  --lang=en
```

Outputs: `en_field_fasttext.npz` and `en_field_tfidf.npz`.

For Chinese,

```shell
python embed_field_text.py \
  ../field-of-study-pipelines/assets/scientific-lit-embeddings/chinese/fasttext/zh_merged_model_011322.bin \
  ../field-of-study-pipelines/assets/scientific-lit-embeddings/chinese/tfidfs/tfidf_model_zh_sample_011222.pkl \
  ../field-of-study-pipelines/assets/scientific-lit-embeddings/chinese/tfidfs/id2word_dict_zh_sample_011222.txt \
  --lang=zh
```

Outputs: `zh_field_fasttext.npz` and `zh_field_tfidf.npz`.
