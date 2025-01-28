"""
Calculate field scores for merged corpus text.
"""
import argparse
import gzip
import json
import timeit
from itertools import zip_longest
from pathlib import Path

from fos.model import FieldModel, Similarity
from fos.settings import CORPUS_DIR


def iter_extract(lang='en', corpus_dir=CORPUS_DIR):
    files = list(Path(corpus_dir).glob(f'{lang}_*.jsonl.gz'))
    assert files
    for file in files:
        with gzip.open(file, 'rb') as infile:
            for line in infile:
                if not line:
                    continue
                yield json.loads(line)


def create_output(
        merged_id: str,
        field_index,
        average_scores: dict,
        similarity: Similarity,
        bq_format=False,
        write_fasttext=False,
        write_entity=False,
        write_tfidf=False,
        exclude_average=False,
        precision=4):
    if bq_format:
        return json.dumps({
            'merged_id': merged_id,
            'fields': [{'id': k, 'score': v} for k, v in average_scores]
        }) + '\n'
    else:
        output = {'merged_id': merged_id}
        if write_fasttext:
            scores = {k: round(float(v), precision) for k, v in zip_longest(field_index, similarity.fasttext)}
            output['fasttext'] = scores
        if write_tfidf:
            scores = {k: round(float(v), precision) for k, v in zip_longest(field_index, similarity.tfidf)}
            output['tfidf'] = scores
        if write_entity:
            scores = {k: round(float(v), precision) for k, v in zip_longest(field_index, similarity.entity)}
            output['entity'] = scores
        if not exclude_average:
            output['fields'] = {k: round(v, precision) for k, v in average_scores}
        return json.dumps(output) + '\n'


def main(lang="en",
         limit=1000,
         corpus=CORPUS_DIR,
         bq_format=False,
         output_path=None,
         write_fasttext=False,
         write_entity=False,
         write_tfidf=False,
         exclude_average=False,
         precision=4):
    if output_path is None:
        output_path = CORPUS_DIR / f'{lang}_scores.jsonl'
    fields = FieldModel(lang)
    start_time = timeit.default_timer()
    i = 0
    with open(output_path, 'wt') as f:
        for record in iter_extract(lang, corpus):
            embedding = fields.embed(record['text'])
            sim = fields.score(embedding)
            avg_sim_values = zip_longest(fields.index, sim.average().astype(float))
            output = create_output(merged_id=record['merged_id'],
                                   field_index=fields.index,
                                   average_scores=avg_sim_values,
                                   similarity=sim,
                                   bq_format=bq_format,
                                   write_fasttext=write_fasttext,
                                   write_entity=write_entity,
                                   write_tfidf=write_tfidf,
                                   exclude_average=exclude_average,
                                   precision=precision)
            f.write(output)
            # if bq_format:
            #     f.write(json.dumps({'merged_id': record['merged_id'],
            #                         'fields': [{'id': k, 'score': v} for k, v in avg_sim_values]}) + '\n')
            # else:
            #     output = {'merged_id': record['merged_id']}
            #     if verbose:
            #         for method in ['fasttext', 'tfidf', 'entity']:
            #             scores = {k: round(float(v), precision) for k, v in
            #                       zip_longest(fields.index, getattr(sim, method))}
            #             output[method] = scores
            #         output['fields'] = {k: round(v, precision) for k, v in avg_sim_values}
            #     else:
            #         output.update({k: round(v, precision) for k, v in avg_sim_values})
            #     f.write(json.dumps(output) + '\n')
            i += 1
            if limit and (i == limit):
                break
    print(round(timeit.default_timer() - start_time))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Score merged corpus text')
    parser.add_argument('lang', choices=('en',), help='Language')
    parser.add_argument('--limit', type=int, default=10000, help='Record limit')
    parser.add_argument('--corpus', default=CORPUS_DIR, help='Directory where corpus to run over exists')
    parser.add_argument('--bq_format', action='store_true', help='If specified, will output nested field scores')
    parser.add_argument('-o', '--output', help='Output path')
    parser.add_argument('-f', '--fasttext', action='store_true', help='Write fastText scores to output')
    parser.add_argument('-e', '--entity', action='store_true', help='Write entity scores to output')
    parser.add_argument('-t', '--tfidf', action='store_true', help='Write tf-idf scores to output')
    parser.add_argument('--exclude_average', action='store_true', help='Omit average scores from output')
    parser.add_argument('-p', '--precision', type=int, default=4, help='Limit precision to digits')
    args = parser.parse_args()
    main(lang=args.lang,
         limit=args.limit,
         corpus=args.corpus,
         bq_format=args.bq_format,
         output_path=args.output,
         write_fasttext=args.fasttext,
         write_entity=args.entity,
         write_tfidf=args.tfidf,
         exclude_average=args.exclude_average,
         precision=args.precision)
