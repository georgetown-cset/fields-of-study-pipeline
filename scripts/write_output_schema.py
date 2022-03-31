"""
Create the BQ schema needed for wide TSV scoring output.
"""
import json
import os
from pathlib import Path

from fos.model import FieldModel

if Path.cwd().name == 'scripts':
    os.chdir('..')


def main(lang="en", path=None):
    fields = FieldModel(lang)
    schema = create_schema(fields)
    if path is None:
        print(json.dumps(schema, indent=2))
        return
    with open(path, 'wt') as f:
        json.dump(schema, f, indent=2)
    print(f"Wrote schema to {path}")


def create_schema(fields):
    schema = [
        {
            "mode": "REQUIRED",
            "name": "merged_id",
            "type": "STRING",
            "description": "Merged-corpus publication ID (as in gcp_cset_links_v2)"
        },
    ]
    for field_id in fields.index:
        schema.append({
            "name": f"field_{field_id}",
            "type": "FLOAT64",
            "description": f"Score for field {field_id}",

        })
    return schema


if __name__ == '__main__':
    for lang in ["en", "zh"]:
        main(lang, f"./schemas/{lang}_output.json")
