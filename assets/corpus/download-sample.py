"""
Download some input records for use in testing.
"""
import pandas_gbq as pbq


def main():
    corpus = pbq.read_gbq(
        "SELECT merged_id, text FROM staging_fields_of_study_v2.en_corpus LIMIT 1000",
    )
    corpus.to_json("en_corpus_example.jsonl.gz", orient='records', lines=True)


if __name__ == "__main__":
    main()
