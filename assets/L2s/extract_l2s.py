"""
Create a few spreadsheets containing L2 fields alongside their L0 and L2 ancestors.
"""
import gzip

from fos.settings import ASSETS_DIR
import pandas as pd

def main():
    # These extracts are originally from the MAG tables FieldsOfStudy and FieldsOfStudyChildren
    with gzip.open(ASSETS_DIR / 'fields/fos.pkl.gz') as zipfile:
        fos = pd.read_pickle(zipfile)
    fos = fos[['display_name', 'level']].copy()

    with gzip.open(ASSETS_DIR / 'fields/fos_children.pkl.gz') as zipfile:
        children = pd.read_pickle(zipfile)

    # Successive joins with the children table give us the L0 fields along with their L1 child IDs and L2 grandchild IDs
    l0 = fos.loc[fos.level == 0].copy()
    l0_l1 = l0.merge(children, left_index=True, right_index=True).rename(columns={'child_id': 'l1_id'})
    l0_l2 = l0_l1.merge(children, left_on='l1_id', right_index=True).rename(columns={'child_id': 'l2_id'})
    l0_l2 = l0_l2.reset_index().rename(columns={'id': 'l0_id'})
    l0_l2 = l0_l2.merge(fos, left_on='l1_id', right_index=True, suffixes=('_l0', '_l1'))
    l0_l2 = l0_l2.merge(fos, left_on='l2_id', right_index=True, suffixes=('_l1', '_l2'))
    # Tidy up and we're done
    l0_l2 = l0_l2.rename(columns={
        "display_name_l0": 'l0_name',
        "display_name_l1": 'l1_name',
        "display_name": 'l2_name',
    })
    # Just need field names and IDs here
    l0_l2 = l0_l2[['l0_name', 'l1_name', 'l2_name', 'l0_id', 'l1_id', 'l2_id']]
    l0_l2 = l0_l2.sort_values(['l0_name', 'l1_name', 'l2_name',])

    # All L2s
    l0_l2.to_csv(ASSETS_DIR / "L2s/L2s.csv", index=False)

    # L2s under more likely L0s
    for l0_field in [
        'Biology',
        'Computer science',
        'Engineering',
    ]:
        l0_children = l0_l2.query(f"l0_name == '{l0_field}'")
        print(f'{len(l0_children):,} L2 children of {l0_field}')
        l0_children.to_csv(ASSETS_DIR / f"L2s/L2s - {l0_field}.csv", index=False)

    # Others:
    # 'Art',
    # 'Business',
    # 'Chemistry',
    # 'Economics',
    # 'Environmental science',
    # 'Geography',
    # 'Geology',
    # 'History',
    # 'Materials science',
    # 'Mathematics',
    # 'Medicine',
    # 'Philosophy',
    # 'Physics',
    # 'Political science',
    # 'Psychology',
    # 'Sociology',


if __name__ == "__main__":
    main()
