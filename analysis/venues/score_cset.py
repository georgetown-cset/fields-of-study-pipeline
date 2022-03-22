"""
Produce v2 field scores for the annotated sample of docs.
"""
import json

from fos.model import FieldModel


def main():
    field_model = FieldModel("en")
    with open("venue_text.jsonl", "rt") as infile, open("venue_text_cset_scores.jsonl", "wt") as outfile:
        for line in infile:
            record = json.loads(line)
            embedding = field_model.embed(record["text"])
            scores = field_model.score(embedding)
            avg_scores = scores.average()
            output = {
                "id": record["merged_id"],
                "fields": [{"id": int(k), "score": x} for k, x in zip(field_model.index, avg_scores)]
            }
            outfile.write(json.dumps(output) + "\n")


if __name__ == '__main__':
    main()
