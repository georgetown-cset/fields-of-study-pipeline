"""
Create an entity keyword trie for fast entity-mention search.
"""
import argparse
import pickle
from pathlib import Path
import numpy as np

from ahocorasick import Automaton

from fos.keywords import read_trie

ASSETS_DIR = (Path(__file__).parent.parent / 'assets').absolute()


def main(entity_path, output_path):
    trie = Automaton()
    # add entity string keys and corresponding values to the trie ...
    for k, v in read_trie(entity_path):
        entity = ' '.join(k)
        vector = np.array(v, dtype=np.float32)
        trie.add_word(entity, (entity, vector))
    # convert the trie to an Aho-Corasick automaton to enable Aho-Corasick search
    trie.make_automaton()
    # write to disk
    with open(output_path, 'wb') as f:
        pickle.dump(trie, f)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('entity_path', type=Path)
    parser.add_argument('output_path', type=Path)
    args = parser.parse_args()
    main(args.entity_path, args.output_path)
