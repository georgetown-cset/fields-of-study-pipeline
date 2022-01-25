"""
Download EN or ZH merged corpus text.
"""
import argparse
import os
from pathlib import Path

from fos.corpus import CORPUS_DIR, download

if Path.cwd().name == 'scripts':
    os.chdir('..')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download merged corpus text')
    parser.add_argument('lang', choices=('en', 'zh'), help='Language')
    parser.add_argument('--output', type=Path, default=CORPUS_DIR, help='Output directory')
    parser.add_argument('--limit', type=int, default=0, help='Record limit')
    parser.add_argument('--skip_prev', type=bool, action='store_true', help='If true, skips unchanged records')
    args = parser.parse_args()
    download(lang=args.lang, output_dir=args.output, limit=args.limit, skip_prev=args.skip_prev)
