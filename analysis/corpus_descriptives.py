"""
WIP on corpus descriptives.
"""
import gzip
import json
from pathlib import Path
from functools import partial


import pandas as pd
import pandas_gbq
import seaborn as sns


def main():
    v1_l0_counts = query_field_counts('fields_of_study')
    v2_l0_counts = query_field_counts('fields_of_study_v2')
    plot_bar(v1_l0_counts, 'v1.png')
    plot_bar(v2_l0_counts, 'v2.png')
    l0_counts = pd.merge(v1_l0_counts, v2_l0_counts, how='outer', on='field_name', suffixes=('_v1', '_v2'))
    l0_props_long = l0_counts[['field_name', 'prop_v1', 'prop_v2']]. \
        melt(id_vars='field_name')
    l0_props_long['variable'] = l0_props_long['variable'].replace('prop_', '')
    l0_props_long['value'].fillna(0.0, inplace=True)
    l0_props_long.reset_index(inplace=True)
    g = sns.catplot(data=l0_props_long.sort_values(['value', 'variable'], ascending=[False, True]),
                    kind="bar",
                    x="value",
                    y="field_name",
                    hue='variable',
                    alpha=.6,
                    height=6,
                    orient='horizontal')
    g.savefig('l0.png')


def query_field_counts(dataset='fields_of_study'):
    sql = f"""\
        -- We'll restrict to non-imputed field scores
        with not_imputed as (
          select distinct merged_id
          from `gcp-cset-projects.{dataset}.field_scores`
          where not is_imputed
        ),
        -- Get top level-0 field counts
        counts as (
            select 
              field.name field_name,
              count(*) n
            from `gcp-cset-projects.{dataset}.top_fields`,
              unnest(fields) as field
            inner join not_imputed using(merged_id)
            where field.level = 0
            group by field_name
            order by n desc
        ),
        total as (
            select sum(n) n_total
        from counts
        )
        select 
            counts.*,
            n / n_total as prop
        from counts
        left join total on true
    """
    return pandas_gbq.read_gbq(sql, project_id='gcp-cset-projects', dialect='standard')


catplot = partial(sns.catplot,
                  kind="bar",
                  x="prop",
                  y="field_name",
                  alpha=.6,
                  height=6,
                  orient='horizontal')


def plot_bar(df, path):
    g = catplot(data=df.sort_values('n', ascending=False))
    g.set_axis_labels("Field", "Publications")
    g.savefig(path)


def iter_extract(pattern):
    paths = list(Path(__file__).parent.glob(pattern))
    assert len(paths)
    for path in paths:
        with gzip.open(path, 'rt') as f:
            for line in f:
                yield json.loads(line)


if __name__ == '__main__':
    main()
