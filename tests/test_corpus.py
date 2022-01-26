from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from fos.corpus import download


@pytest.mark.skip(reason="very slow")
def test_download():
    for lang in ['en', 'zh']:
        with TemporaryDirectory() as tmpdir:
            download(lang, output_dir=Path(tmpdir), limit=100)
