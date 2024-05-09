"""
Visualize field embeddings.
"""
import os
from pathlib import Path

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from adjustText import adjust_text
from matplotlib import font_manager
from sklearn.manifold import TSNE
from sklearn.metrics.pairwise import cosine_similarity

from fos.vectors import load_field_fasttext, load_field_keys

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

def main(lang='en'):
    # Load the matrix of field FastText vectors
    # The field vectors form a matrix with {field count} rows and {FastText dimensionality} columns.
    vectors = load_field_fasttext(lang).index

    # Load the row order as indicated by field IDs
    keys = load_field_keys(lang)

    # Sort the field attributes table in the field matrix's row order
    # Our dataframe of field attributes gives the field names for humans, along with the field levels
    attrs = pd.read_pickle('../assets/fields/fos.pkl.gz')
    attrs = attrs.loc[keys]
    assert vectors.shape[0] == attrs.shape[0]

    # Child-parent table for e.g. plotting the L1 children of L0 'Computer science'
    parents = create_parent_table(attrs)

    # Zeroed rows are fields without embeddings
    is_zeroed = np.array([(row == 0).all() for row in vectors])
    # if lang == 'zh':
    #     is_zeroed = np.array((attrs.level == 1).values)
    if is_zeroed.any():
        vectors = np.delete(vectors, is_zeroed, axis=0)
        keys = list(np.array(keys)[~is_zeroed])
        attrs = attrs.loc[keys]
    parents = parents.loc[parents.child_id.isin(keys)]

    # Add Nunito Sans to matplotlib's font manager
    load_fonts()

    # Fit t-SNE on field vectors
    tsne_df = fit_tsne(vectors, keys, attrs)

    # Plot L0 embeddings in 2d
    plot_l0_scatter(tsne_df, lang)
    # For each L0, plot the 2d coords of its L1 children
    plot_l1_scatter(tsne_df, parents, lang)

    # Plot the cosine similarities of the L0 embeddings as a heatmap
    plot_l0_heatmap(vectors, attrs, lang)
    # Same, just for STEM fields
    plot_stem_heatmap(vectors, attrs, lang)
    # For each L0, plot the cosine similarities of its L1 children's embeddings as a heatmap
    plot_l1_heatmaps(vectors, parents, attrs, lang)


def create_parent_table(attrs):
    # Load parent-child table
    children = pd.read_pickle('../assets/fields/fos_children.pkl.gz')
    # Create parent -> child table with parent names
    parents = attrs.loc[attrs.level == 0, ['display_name']]. \
        rename(columns={'display_name': 'parent_name'}). \
        merge(children, left_index=True, right_index=True, how='inner')
    return parents


def fit_tsne(vectors, keys, attrs):
    # Fit t-SNE on field vectors
    tsne_coords = TSNE(
        random_state=20211205,
        init='pca',
        learning_rate='auto').fit_transform(vectors)
    tsne_df = pd.DataFrame(tsne_coords)
    tsne_df.rename(columns={0: 'x', 1: 'y'}, inplace=True)
    tsne_df.index = keys
    assert tsne_df.shape[0] == attrs.shape[0]
    tsne_df = tsne_df.merge(attrs, left_index=True, right_index=True, how='inner')
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


def plot_tsne(tsne_df):
    sns.set_theme('notebook', 'white', rc={"figure.figsize": (4, 4)})

    plt.figure(figsize=(8, 8))
    plt.rcParams['font.family'] = 'sans-serif'
    plt.rcParams['font.sans-serif'] = FONT_NAME

    scatter = sns.scatterplot(x='x', y='y', data=tsne_df, s=25, legend=False)
    scatter.set_xlabel('t-SNE x')
    scatter.set_ylabel('t-SNE y')
    texts = []
    for i in range(0, tsne_df.shape[0]):
        text = scatter.text(tsne_df.x[i],
                            tsne_df.y[i],
                            tsne_df['display_name'][i],
                            horizontalalignment='left',
                            color='black')
        texts.append(text)
    adjust_text(texts, arrowprops=dict(arrowstyle="-", color='gray', lw=1))
    return scatter


def plot_l0_scatter(tsne_df, lang: str):
    # Plot level-0 fields
    set_scale(1.25)
    plot_tsne(tsne_df.query('level == 0'))
    set_title(f'Level-0 Field Embeddings ({lang.upper()})')
    save(f'{lang}-scatter-level-0.{FILE_TYPE}')


def plot_l1_scatter(tsne_df, parents, lang):
    # Plot level 1 child fields of each parent
    set_scale(1)
    for parent_name, children in parents.groupby('parent_name'):
        child_tsne = tsne_df.loc[children['child_id']]
        plot_tsne(child_tsne)
        set_title(f'{parent_name}: Level-1 Field Embeddings ({lang.upper()})')
        save(f'{lang}-scatter-level-1-{parent_name}.{FILE_TYPE}')


def sim_table(vectors, attrs, mask):
    # Calculate + shape similarities for heatmap
    sim = pd.DataFrame(cosine_similarity(vectors[mask, :]))
    sim.index = attrs.loc[mask, 'display_name'].rename('Field')
    sim.sort_index(inplace=True)
    sim.columns = attrs.loc[mask, 'display_name'].rename('Field')
    sim = sim[sorted(sim.columns)]
    return sim


def plot_heatmap(sim_df, annot=True):
    # Plot a heatmap of similarities
    f, ax = plt.subplots(figsize=(10, 10))
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


def plot_l0_heatmap(vectors, attrs, lang):
    # L0 heatmap
    l0_sim = sim_table(vectors, attrs, attrs.level == 0)
    plot_heatmap(l0_sim, annot=False)
    set_title(f'Level-0 Field Similarities ({lang.upper()})')
    plt.subplots_adjust(left=.22, bottom=.20)
    save(f'{lang}-heatmap-level-0.{FILE_TYPE}')
    plt.close()


def plot_stem_heatmap(vectors, attrs, lang):
    # STEM heatmap
    stem_mask = attrs.display_name.isin(L0_FIELDS['stem'])
    assert stem_mask.sum() == len(L0_FIELDS['stem'])
    stem_sim = sim_table(vectors, attrs, stem_mask)
    plot_heatmap(stem_sim, annot=False)
    set_title(f'STEM: Level-0 Field Similarities ({lang.upper()})')
    plt.subplots_adjust(left=.22, bottom=.20)
    save(f'{lang}-heatmap-level-0-stem.{FILE_TYPE}')
    plt.close()


def plot_l1_heatmaps(vectors, parents, attrs, lang):
    # Level 1 heatmaps by L0 parent
    for parent_name, children in parents.groupby('parent_name'):
        scale = min([1.5, max([.75, 15 / children.shape[0]])])
        if parent_name in ['Chemistry', 'Geology', 'Mathematics']:
            scale = .9
        sns.set_theme(font_scale=scale, font='Nunito Sans')
        child_mask = attrs.index.isin(children.child_id)
        child_sim = sim_table(vectors, attrs, child_mask)
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
    for lang in ['zh', 'en']:
        main(lang)
