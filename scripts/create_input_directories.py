"""
Chunk up the corpus files into directories, for hacky parallel processing.
"""

from shutil import copy2

import typer
from more_itertools import chunked
from fos.settings import CORPUS_DIR


def main(lang="en", n_chunks: int = 80):
    glob = f'{lang}*.jsonl.gz'
    files = list(CORPUS_DIR.glob(glob))
    if not files:
        raise FileNotFoundError(f"No files found in {CORPUS_DIR} match glob {glob}")
    chunk_size = max(1, len(files) // n_chunks)
    chunks = list(chunked(files, chunk_size))
    for i, chunk in enumerate(chunks):
        chunk_dir = CORPUS_DIR / f'{lang}_chunk_{i}'
        chunk_dir.mkdir(exist_ok=True)
        for file in chunk:
            copy2(file, chunk_dir / file.name)
        print(f'Copied {len(chunk)} files to {chunk_dir}')


if __name__ == "__main__":
    typer.run(main)
