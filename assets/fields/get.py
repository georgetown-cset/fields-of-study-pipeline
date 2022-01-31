"""
Retrieve the latest field metadata from BQ.
"""
import gzip
import os
import pickle
from pathlib import Path

import networkx as nx
import pandas as pd

DATA_DIR = Path(__file__).parent

os.environ['GCLOUD_PROJECT'] = 'gcp-cset-projects'

tables = {
    'fos': """\
        SELECT
          FieldOfStudyId AS id,
          NormalizedName AS normalized_name,
          DisplayName AS display_name,
          MainType AS main_type,
          Level AS level,
          CreatedDate AS created_date
        FROM gcp_cset_mag.FieldsOfStudy""",
    'fos_children': """\
        SELECT
          FieldOfStudyId AS id,
          ChildFieldOfStudyId AS child_id
        FROM gcp_cset_mag.FieldOfStudyChildren""",
    'fos_attr': """\
        select
          FieldOfStudyId id,
          AttributeType type,
          AttributeValue value
        from gcp_cset_mag.FieldOfStudyExtendedAttributes""",
}


def read_table(name: str) -> pd.DataFrame:
    try:
        df = pd.read_pickle(DATA_DIR / f'{name}.pkl.gz')
        print(f'Read {name} from disk')
        return df
    except FileNotFoundError:
        df = pd.read_gbq(tables[name], index_col='id')
        df.to_pickle(DATA_DIR / f'{name}.pkl.gz')
        return df


def main():
    fos = read_table('fos')
    fos_children = read_table('fos_children')
    fos_attr = read_table('fos_attr')

    # Create an nx DAG from FieldsOfStudy + FieldOfStudyChildren
    try:
        dag = pickle.loads(gzip.decompress(Path(DATA_DIR, 'dag.pkl.gz').read_bytes()))
        print('Read dag from disk')
    except FileNotFoundError:
        dag: nx.DiGraph = nx.convert_matrix.from_pandas_edgelist(
            fos_children.reset_index(), 'id', 'child_id', create_using=nx.DiGraph)
        for i, row in fos.iterrows():
            dag.add_node(i, **row)
        Path(DATA_DIR, 'dag.pkl.gz').write_bytes(gzip.compress(pickle.dumps(dag)))

    # Which FoS in the FoS table appear (anywhere) in the parent-child table?
    ids_in_children = pd.concat([
        pd.Series(fos_children.index),
        fos_children['child_id']
    ]).unique()

    print(f'Fields: {fos.shape[0]:,}')
    print(f'Fields in parent-child table: {len(ids_in_children):,}')
    print(f'Fields with extended attributes: {len(fos_attr.index.unique()):,}')
    print(f'Nodes in DAG: {dag.number_of_nodes():,}')
    print(f'Edges in DAG: {dag.number_of_edges():,}')
    print('\nFields by level:')
    print(fos['level'].value_counts(ascending=True, dropna=False))


if __name__ == '__main__':
    main()
