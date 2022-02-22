import json
from itertools import zip_longest


def test_results():
    with open('../scripts/batch.json', 'rt') as batch, open('../scripts/stream.json', 'rt') as stream:
        i = 0
        for i, (batch_row, stream_row) in enumerate(zip_longest(batch, stream)):
            batch_record = json.loads(batch_row)
            stream_record = json.loads(stream_row)
            assert batch_record['merged_id'] == stream_record['merged_id']
            stream_fields = {field['id']: field['score'] for field in stream_record['fields']}
            batch_fields = {field['id']: field['score'] for field in batch_record['fields']}
            for k, v in batch_fields.items():
                assert round(stream_fields[k], 4) == round(v, 4), (stream_fields[k], v)
        assert i > 0
