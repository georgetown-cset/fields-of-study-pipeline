from pathlib import Path
from tempfile import TemporaryDirectory

from fos.corpus import download


def test_download():
    for lang in ['en', 'zh']:
        with TemporaryDirectory() as tmpdir:
            download(lang, output_dir=Path(tmpdir), limit=100)
