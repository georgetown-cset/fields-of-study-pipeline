"""
Visualize field embeddings.
"""
import os
from pathlib import Path

import dataset
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from adjustText import adjust_text
from matplotlib import font_manager
from sklearn.manifold import TSNE
from sklearn.metrics.pairwise import cosine_similarity

from fos.vectors import load_field_fasttext, load_field_keys, load_field_entities

db = dataset.connect('sqlite:///../wiki/data/wiki.db')
table = db['pages']

FIG_DIR = 'field_embeddings'
L0_FIELDS = {
    'humanities': ['Art', 'History', 'Philosophy'],
    'social_sciences': ['Business', 'Economics', 'Geography', 'Political science', 'Psychology', 'Sociology'],
    'stem': ['Biology', 'Chemistry', 'Computer science', 'Engineering', 'Environmental science', 'Geology',
             'Materials science', 'Mathematics', 'Medicine', 'Physics']
}
FONT_PATH = 'fonts/NunitoSans-Black.ttf'
FONT_NAME = 'Nunito Sans'
FILE_TYPE = 'png'


def main(vectors, lang='en'):
    # Load the field embedding matrix row order as indicated by field display names
    keys = load_field_keys(lang)
    levels = pd.DataFrame(db.query("select distinct display_name, level from pages"))
    assert not levels["display_name"].duplicated().any()
    assert levels["display_name"].isin(keys).all()
    assert levels["level"].isin([0, 1, 2, 3]).all()
    assert len(keys) == len(levels)

    # Get the parent-child relations between fields
    children = pd.read_json('../assets/fields/all_fields_hierarchy.jsonl', lines=True)

    # Add Nunito Sans to matplotlib's font manager
    load_fonts()

    # Fit t-SNE on field vectors
    tsne_df = fit_tsne(vectors, keys)

    # Add levels
    assert len(tsne_df) == len(levels)
    tsne_df = tsne_df.merge(levels, left_index=True, right_on='display_name', how='inner')

    # Plot L0 embeddings in 2d
    plot_l0_scatter(tsne_df, lang)
    # For each L0, plot the 2d coords of its L1 children
    # plot_l1_scatter(tsne_df, parents, lang)

    # For each L0, plot the 2d coords of its L1 children WITH THE PARENT
    plot_l1_scatter(tsne_df, children, lang, plot_parent=True)

    # Plot the cosine similarities of the L0 embeddings as a heatmap
    plot_l0_heatmap(vectors, keys, levels, lang)
    # Same, just for STEM fields
    plot_stem_heatmap(vectors, keys, levels, lang)
    # For each L0, plot the cosine similarities of its L1 children's embeddings as a heatmap
    plot_l1_heatmaps(vectors, children, keys, levels, lang)


def fit_tsne(vectors, keys):
    # Fit t-SNE on field vectors
    tsne_coords = TSNE(
        random_state=20211205,
        init='pca',
        learning_rate='auto').fit_transform(vectors)
    tsne_df = pd.DataFrame(tsne_coords)
    tsne_df.rename(columns={0: 'x', 1: 'y'}, inplace=True)
    tsne_df.index = keys
    assert not tsne_df.index.duplicated().any()
    return tsne_df


def load_fonts():
    # Add Nunito Sans to matplotlib's font manager
    font_props = {}
    for font_path in Path('fonts').glob('*.ttf'):
        name = str(font_path)
        font_manager.fontManager.addfont(name)
        prop = font_manager.FontProperties(fname=name)
        font_props[name] = prop
    return font_props


def plot_tsne(tsne_df, parent_tsne=None, neighbors_tsne=None, size=20, **kw):
    """
    PARAMETERS
    parent_tsne = parent df values or None if there's no parent point

    Modify the input tsne_df
    """
    tsne_df = tsne_df.copy()

    sns.set_theme('notebook', 'white')

    plt.figure(figsize=(size, size))
    plt.rcParams['font.family'] = 'sans-serif'
    plt.rcParams['font.sans-serif'] = FONT_NAME

    tsne_df['relationship'] = 'Child'
    tsne_df['text_color'] = 'blue'

    # plot parent point
    if parent_tsne is not None:
        parent_tsne = parent_tsne.copy()
        parent_tsne['relationship'] = 'Parent'
        parent_tsne['text_color'] = 'red'
        tsne_df = pd.concat([tsne_df, parent_tsne], ignore_index=True)

    if neighbors_tsne is not None:
        neighbors_tsne = neighbors_tsne.copy()
        neighbors_tsne['relationship'] = 'Neighbor'
        neighbors_tsne['text_color'] = 'green'
        tsne_df = pd.concat([tsne_df, neighbors_tsne], ignore_index=True)

    scatter = sns.scatterplot(x='x', y='y', data=tsne_df, s=25, hue='relationship', legend=False, **kw)

    scatter.set_xlabel('t-SNE x')
    scatter.set_ylabel('t-SNE y')
    texts = []
    for i, row in tsne_df.iterrows():
        text = scatter.text(row["x"],
                            row["y"],
                            row['display_name'],
                            horizontalalignment='left',
                            color=row['text_color'])
        texts.append(text)

    adjust_text(texts, arrowprops=dict(arrowstyle="-", color='gray', lw=1))
    return scatter


def plot_l0_scatter(tsne_df, lang: str, **kw):
    # Plot level-0 fields
    set_scale(1.25)
    plot_tsne(tsne_df.query('level == 0'), **kw)
    set_title(f'Level-0 Field Embeddings ({lang.upper()})')
    save(f'{lang}-scatter-level-0.{FILE_TYPE}')
    plt.close()


def plot_l1_scatter(tsne_df, parents, lang, plot_parent=False, **kw):
    # Plot level 1 child fields of each parent
    # set plot_parent = True to plot the parent point on the graph
    set_scale(1)
    for parent_name, children in parents.groupby('display_name'):
        # Subset the tsne df to only include the children of the parent
        child_tsne = tsne_df.loc[tsne_df["display_name"].isin(children['child_display_name'])]
        set_title(f'{parent_name}: Level-1 Field Embeddings ({lang.upper()})')

        if plot_parent:
            parent_tsne = tsne_df.query(f'level == 0 & display_name == "{parent_name}"')
            outfilename = f'{lang}-scatter-parent-level-1-{parent_name}.{FILE_TYPE}'
        else:
            parent_tsne = None
            outfilename = f'{lang}-scatter-level-1-{parent_name}.{FILE_TYPE}'
        plot_tsne(child_tsne, parent_tsne=parent_tsne, **kw)
        save(outfilename)
        plt.close()


def sim_table(vectors, keys, mask):
    # Calculate + shape similarities for heatmap
    sim = pd.DataFrame(cosine_similarity(vectors[mask, :]))
    sim.index = keys
    sim.sort_index(inplace=True)
    sim.columns = keys
    sim = sim[sorted(sim.columns)]
    return sim


def plot_heatmap(sim_df, annot=True):
    # Plot a heatmap of similarities
    f, ax = plt.subplots(figsize=(20, 20))
    plt.rcParams['font.family'] = 'sans-serif'
    plt.rcParams['font.sans-serif'] = FONT_NAME
    sns.heatmap(sim_df,
                annot=annot,
                fmt='.2f',
                cmap="magma",
                cbar=False)
    xlocs, xlabels = plt.xticks()
    plt.setp(xlabels, rotation=45, ha='right')
    plt.xlabel(None)
    ylocs, ylabels = plt.yticks()
    plt.setp(ylabels, rotation=0, ha='right')
    plt.ylabel(None)
    return ax


def plot_l0_heatmap(vectors, keys, levels, lang):
    # L0 heatmap
    # Get a vector mask for L0 fields
    mask = [k in levels.loc[levels.level == 0, 'display_name'].values for k in keys]
    # Get keys in correct order
    l0_keys = [k for k, is_l0 in zip(keys, mask) if is_l0]
    l0_sim = sim_table(vectors, l0_keys, mask)
    plot_heatmap(l0_sim, annot=False)
    set_title(f'Level-0 Field Similarities ({lang.upper()})')
    plt.subplots_adjust(left=.22, bottom=.20)
    save(f'{lang}-heatmap-level-0.{FILE_TYPE}')
    plt.close()


def plot_stem_heatmap(vectors, keys, levels, lang):
    mask = [
        k in levels.loc[
            (levels.level == 0) & (levels.display_name.isin(L0_FIELDS['stem'])), 'display_name'
        ].values for k in keys
    ]
    stem_keys = [k for k, include in zip(keys, mask) if include]
    stem_sim = sim_table(vectors, stem_keys, mask)
    plot_heatmap(stem_sim, annot=False)
    set_title(f'STEM: Level-0 Field Similarities ({lang.upper()})')
    plt.subplots_adjust(left=.22, bottom=.20)
    save(f'{lang}-heatmap-level-0-stem.{FILE_TYPE}')
    plt.close()


def plot_l1_heatmaps(vectors, parents, keys, levels, lang):
    # Level 1 heatmaps by L0 parent
    for parent_name, children in parents.groupby('display_name'):
        scale = min([1.5, max([.75, 15 / children.shape[0]])])
        if parent_name in ['Chemistry', 'Geology', 'Mathematics']:
            scale = .9
        sns.set_theme(font_scale=scale, font='Nunito Sans')
        mask = [k in children['child_display_name'].values for k in keys]
        child_keys = [k for k, include in zip(keys, mask) if include]
        child_sim = sim_table(vectors, child_keys, mask)
        plot_heatmap(child_sim, annot=False)
        set_title(f'{parent_name}: Level-1 Field Similarities ({lang.upper()})')
        plt.subplots_adjust(left=.22, bottom=.20)
        save(f'{lang}-heatmap-level-1-{parent_name}.{FILE_TYPE}')
        plt.close()


def set_title(title):
    plt.title(title, fontdict={'size': 15})


def set_scale(scale):
    sns.set(font_scale=scale, font=FONT_PATH)


def save(name):
    plt.savefig(os.path.join(FIG_DIR, name), bbox_inches='tight')


if __name__ == '__main__':
    lang = "en"
    # Load the matrix of field vectors
    # The field vectors form a matrix with {field count} rows and {FastText dimensionality} columns.
    FIG_DIR = 'field_embeddings/fasttext'
    ft_vectors = load_field_fasttext(lang).index
    main(ft_vectors, lang)

    # Same for entities
    FIG_DIR = 'field_embeddings/entity'
    entity_vectors = load_field_entities(lang).index
    main(entity_vectors, lang)
