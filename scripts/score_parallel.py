import json
import os
import timeit
from datetime import datetime as dt
from itertools import zip_longest
from multiprocessing import Process, Queue

from tqdm import tqdm
import typer
from more_itertools import chunked

from fos.model import FieldModel
from fos.util import iter_bq_extract


def score(text, model):
    embedding = model.embed(text)
    sim = model.score(embedding)
    avg_sim_values = zip_longest(model.index, sim.average().astype(float))
    return [{'id': k, 'score': v} for k, v in avg_sim_values]


def worker(input, output, lang, worker_id):
    print(f'[{dt.now().isoformat()}] Loading assets on worker {worker_id} with PID {os.getpid()}')
    model = FieldModel(lang)
    for i, record in enumerate(iter(input.get, 'STOP'), 1):
        scores = score(record['text'], model)
        output.put({"merged_id": record['merged_id'], "fields": scores})
    print(f"Worker {worker_id} shutdown")


def main(lang="en", batch_size: int = 1_000, limit: int = 10_000, max_workers: int = os.cpu_count()):
    i = 0
    start_time = timeit.default_timer()
    print(f'[{dt.now().isoformat()}] Starting job with {max_workers} workers')

    # Create queues
    tasks = Queue()
    completed_tasks = Queue()

    # Start workers
    for worker_id in range(max_workers):
        Process(target=worker, args=(tasks, completed_tasks, lang, worker_id)).start()

    # Iterate over inputs in batches with the specified size, writing a JSONL output file for each batch
    batch_index = 0
    for batch in chunked(iter_bq_extract(f'{lang}_'), batch_size):
        batch_start_time = timeit.default_timer()

        # Pass them tasks
        for record in batch:
            tasks.put(record)

        # Iterate over completed tasks, writing them to disk
        with open(f'{lang}_scores_{batch_index}.jsonl', 'wt') as f:
            for _ in range(len(batch)):
                output = json.dumps(completed_tasks.get()) + '\n'
                f.write(output)

        i += len(batch)
        batch_index += 1

        batch_stop_time = timeit.default_timer()
        batch_elapsed = round(batch_stop_time - batch_start_time, 1)
        print(f'[{dt.now().isoformat()}] Scored {len(batch):,} docs in {batch_elapsed}s ({i:,} scored so far)')

        if limit and (i >= limit):
            print(f'[{dt.now().isoformat()}] Stopping (--limit was {limit:,})')
            break

    # Clean up
    for _ in range(max_workers):
        tasks.put('STOP')

    stop_time = timeit.default_timer()
    elapsed = round(stop_time - start_time, 1)
    print(f'[{dt.now().isoformat()}] Scored {i:,} docs in {elapsed}s')


if __name__ == '__main__':
    typer.run(main)
