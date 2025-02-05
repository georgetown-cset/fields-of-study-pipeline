"""
Download EN merged corpus text.
"""
import argparse
import os
from pathlib import Path

from fos.corpus import CORPUS_DIR, download

if Path.cwd().name == 'scripts':
    os.chdir('..')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download merged corpus text')
    parser.add_argument('lang', choices=('en',), help='Language')
    parser.add_argument('--output', type=Path, default=CORPUS_DIR, help='Output directory')
    parser.add_argument('--limit', type=int, default=0, help='Record limit')
    parser.add_argument('--skip_prev', action='store_true', help='If true, skips unchanged records')
    parser.add_argument('--use_default_clients', action='store_true', help='If true, read creds from environment')
    parser.add_argument('--bq_dest', type=str, default='field_model_replication', help='Dataset in BQ where data should be written')
    parser.add_argument('--extract_bucket', type=str, default='fields-of-study', help='Bucket in GCS where exported jsonl should be written')
    parser.add_argument('--extract_prefix', type=str,
                        help='GCS prefix where exported jsonl should be written within `extract_bucket`')
    args = parser.parse_args()
    download(lang=args.lang, output_dir=args.output, limit=args.limit, skip_prev=args.skip_prev,
             use_default_clients=args.use_default_clients, bq_dest=args.bq_dest, extract_bucket=args.extract_bucket,
             extract_prefix=args.extract_prefix)
