import csv
import json
from collections import defaultdict
import copy

original_level_ones_to_exclude = ["computer science", "biology"]
name_remapping = {"algorithm": "algorithms",
                  "simulation": "computer simulation"}

def read_field_tsv(filename: str, has_hierarchy: bool = False) -> dict:
    """
    Read a field tsv document containing raw field data, including names and wiki info
    :param filename: tsv document name
    :param has_hierarchy: whether the document includes parent names
    :return:
    """
    field_map = {}
    # if the document includes parent names we want these
    if has_hierarchy:
        fields = ["parent_name"]
        # our document with parent names also has multiple wiki title fields not just one
        add_cols = ["wiki_title_2", "wiki_title_3"]
    else:
        # our document without parent names has an id field
        fields = ["id"]
        add_cols = None
    fields.extend(["level", "display_name", "normalized_name", "wiki_title_1"])
    if add_cols:
        fields.extend(add_cols)
    with open(filename, "r") as csvfile:
        reader = csv.DictReader(csvfile, delimiter="\t", fieldnames=fields)
        for i, row in enumerate(reader):
            # we don't want the header row
            if i == 0:
                continue
                # we don't want the id field
            if not has_hierarchy:
                del row["id"]
            field_map[row["normalized_name"]] = row
    return field_map

def read_field_hierarchy(filename: str) -> tuple:
    """
    Ingest the jsonl field hierarchy document
    :param filename: filename containing field hierarchy
    :return: a defaultdict containing the fieldmap, a list of fields that need to be fixed later
    """
    to_exclude = []
    to_fix_later = []
    with open(filename, "r") as json_file:
        data = [json.loads(line) for line in json_file]
    field_map = defaultdict(dict)
    # we're going to do this loop twice; once to find the fields to exclude and once to add entries
    for row in data:
        # We want to exclude the level one children we're replacing from our original hierarchy
        if row["child_level"] == 1 and row["normalized_name"] in original_level_ones_to_exclude:
            to_exclude.append(row["child_normalized_name"])
    for row in data:
        if row["child_normalized_name"] in to_exclude:
            if row["normalized_name"] not in original_level_ones_to_exclude:
                to_fix_later.append(row)
            continue
        field_map[row["child_normalized_name"]][row["normalized_name"]] = row
    return field_map, to_fix_later

def fix_removed_fields(to_fix_later: list, field_hierarchy: defaultdict) -> None:
    """
    Here we want to add back fields into our hierarchy that had the same name as fields in our removed
    level one fields (currently from computer science and biology).
    There are three cases here but we handle two the same: if the field was renamed (handled separately),
    if the field still exists in identical form but is in multiple L0s, or if the field was removed from CS/bio.
    For our purposes we don't care about the difference between the latter two, but we don't want two forms
    of the same fields sticking around, so we need to fix the renamings.
    :param to_fix_later: list of removed field rows to fix
    :param field_hierarchy: defaultdict of the field hierarchy
    :return: None
    """
    for row in to_fix_later:
        name = row["child_normalized_name"]
        if name in name_remapping:
            field_hierarchy[name_remapping[name]][row["normalized_name"]] = {"normalized_name": row["normalized_name"],
            "display_name": row["display_name"], "parent_level": row["parent_level"],
            "child_normalized_name": name_remapping[name], "child_display_name": name_remapping[name].capitalize(),
            "child_level": row["child_level"]}
        else:
            field_hierarchy[name][row["normalized_name"]] = row

def add_new_fields_to_list(initial_fields: dict, manual_field_update: dict, field_hierarchy: defaultdict) -> dict:
    """
    Take the initial fields from MAG and add our manual fields to them, removing fields that we edited in
    in our manual update process
    :param initial_fields: dict of initial fields
    :param manual_field_update: dict of fields from our manual update
    :param field_hierarchy: defaultdict of our field hierarchy
    :return: updated dict of all fields
    """
    # first remove the fields that we excluded from the hierarchy because we're replacing them
    # want to make sure not to remove level 0s as we do this
    new_initial = copy.deepcopy(initial_fields)
    for elem in initial_fields:
        if elem not in field_hierarchy and initial_fields[elem]["level"] != "0":
            del new_initial[elem]
    for new_field in manual_field_update:
        new_initial[new_field] = manual_field_update[new_field]
    return new_initial



def add_new_fields_to_hierarchy(field_hierarchy: defaultdict, manual_field_update: dict, all_fields: dict) -> None:
    """
    Add our new manual fields into our field hierarchy
    :param field_hierarchy: defaultdict of field hierarchy
    :param manual_field_update:
    :param all_fields:
    :return:
    """
    for elem in manual_field_update:
        row = manual_field_update[elem]
        field_hierarchy[elem][row["parent_name"]] = {"normalized_name": row["parent_name"],
            "display_name": all_fields[row["parent_name"]]["display_name"], "parent_level": int(row["level"]) - 1,
            "child_normalized_name": row["normalized_name"], "child_display_name": row["display_name"],
            "child_level": row["level"]}

def write_fields(fields: dict, filename: str) -> None:
    """
    Write out new all fields combined tsv
    :param fields: dict of all fields to write out
    :param filename: filename to write to
    :return: None
    """
    fieldnames = ["level", "display_name", "normalized_name", "wiki_title_1", "wiki_title_2", "wiki_title_3"]
    with open(filename, "w") as writefile:
        writer = csv.DictWriter(writefile, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        for row in fields:
            if "parent_name" in fields[row]:
                del fields[row]["parent_name"]
            writer.writerow(fields[row])

def write_hierarchy(hierarchy: defaultdict, filename: str) -> None:
    """
    Write out new hierarchy jsonl file
    :param hierarchy: defaultdict of hierarchy to write out
    :param filename: filename to write to
    :return: None
    """
    with open(filename, 'w') as f:
        for child_field in hierarchy:
            for field in hierarchy[child_field]:
                f.write(json.dumps(hierarchy[child_field][field]) + "\n")


if __name__ == "__main__":
    initial_fields = read_field_tsv("data/fields.tsv")
    manual_field_update = read_field_tsv("data/levels2and3.tsv", has_hierarchy=True)
    field_hierarchy, to_fix_later = read_field_hierarchy("../assets/fields/level_one_field_hierarchy.jsonl")
    fix_removed_fields(to_fix_later, field_hierarchy)
    all_fields = add_new_fields_to_list(initial_fields, manual_field_update, field_hierarchy)
    add_new_fields_to_hierarchy(field_hierarchy, manual_field_update, all_fields)
    write_fields(all_fields, "data/all_fields.tsv")
    write_hierarchy(field_hierarchy, "../assets/fields/all_fields_hierarchy.jsonl")
