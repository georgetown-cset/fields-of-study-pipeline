import json
from pathlib import Path

from fos.gcp import write_query, download_table
from fos.util import iter_bq_extract, preprocess_text


def main():
    # write_query(Path("corpus.sql"), destination="field_model_replication.top_venue_papers", clobber=True)
    # ~5 GB compressed in ~29 chunks
    # download_table("field_model_replication.top_venue_papers", "fields-of-study-model",
    #                "top-venues/top-venues-2022-03-21", Path("."))
    with open("venue_text.jsonl", "wt") as f:
        for doc in iter_bq_extract("top-venues-", Path(".")):
            output = {
                "id": doc["paper_id"],
                "text": preprocess_text(doc),
            }
            f.write(json.dumps(output) + "\n")


if __name__ == '__main__':
    main()
