import json
import os
import timeit
from datetime import datetime as dt
from itertools import zip_longest
from multiprocessing import Process

import typer
from more_itertools import chunked

from fos.model import FieldModel
from fos.util import iter_bq_extract


def score(text, model):
    embedding = model.embed(text)
    sim = model.score(embedding)
    avg_sim_values = zip_longest(model.index, sim.average().astype(float))
    return [{'id': k, 'score': v} for k, v in avg_sim_values]


def worker(lang, n_workers, worker_id, batch_size, limit):
    print(f'[{dt.now().isoformat()}] Loading assets on worker {worker_id} with PID {os.getpid()}')
    model = FieldModel(lang)

    worker_n = 0
    n = 0
    worker_limit = limit // n_workers
    start_time = timeit.default_timer()

    for batch_id, batch in enumerate(chunked(iter_bq_extract(f'{lang}_'), batch_size)):
        with open(f'{lang}_scores_{batch_id}_{worker_id}.jsonl', 'wt') as f:
            batch_start_time = timeit.default_timer()
            for line_index, record in enumerate(batch):
                if not (line_index - worker_id) % n_workers == 0 or line_index < worker_id:
                    n += 1
                    continue
                scores = score(record['text'], model)
                result = {"merged_id": record['merged_id'], "fields": scores}
                f.write(json.dumps(result) + '\n')
                worker_n += 1

                if limit and (worker_n >= worker_limit):
                    print(f'[{dt.now().isoformat()}] Stopping (--limit was {limit:,})')
                    break
        if limit and (worker_n >= worker_limit):
            break
        batch_elapsed = round(timeit.default_timer() - batch_start_time, 1)
        print(f"[{dt.now().isoformat()}] Worker {worker_id} scored {len(batch)} records ({worker_n} by worker so far) "
              f"in {batch_elapsed}s")

    elapsed = round(timeit.default_timer() - start_time, 1)
    print(f"[{dt.now().isoformat()}] Worker {worker_id} shutdown after {worker_n + 1} records processed in {elapsed}s")


def main(lang="en", batch_size: int = 1_000, limit: int = 10_000, max_workers: int = os.cpu_count()):
    print(f'[{dt.now().isoformat()}] Starting job with {max_workers} workers')
    for worker_id in range(max_workers):
        Process(target=worker, args=(lang, max_workers, worker_id, batch_size, limit)).start()


if __name__ == '__main__':
    typer.run(main)
