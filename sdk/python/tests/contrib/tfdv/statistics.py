import os
import shutil
import tempfile
import urllib
import zipfile

import pytest


@pytest.yield_fixture(scope="module")
def training_data():
    temp_dir = tempfile.mkdtemp()
    data_dir = os.path.join(temp_dir, 'data')
    training_data = os.path.join(data_dir, 'train', 'data.csv')

    zip, headers = urllib.request.urlretrieve('https://storage.googleapis.com/artifacts.tfx-oss-public.appspot.com/datasets/chicago_data.zip')
    zipfile.ZipFile(zip).extractall(temp_dir)
    zipfile.ZipFile(zip).close()
    yield training_data
    shutil.rmtree(temp_dir)


def test_generate_statistics_from_parquet():
    pass