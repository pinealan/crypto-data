import os
import time
import shutil
from pathlib import Path
from datetime import datetime

import pytest

from datasink import Datasink


# default test configs
root       = './__test'
ext        = 'csv'
resolution = Datasink.MONTH


def test_datasink_day_create_file():
    resolution = Datasink.DAY
    sink = Datasink(root=root, ext=ext, resolution=resolution)
    sink.write('test')

    today = datetime.today().strftime('%Y/%m/%d')
    assert os.path.isdir(root)
    assert os.path.isfile('{}/{}.{}'.format(root, today, ext))

    del sink
    shutil.rmtree(root)


def test_datasink_month_create_file():
    resolution = Datasink.MONTH
    sink = Datasink(root=root, ext=ext, resolution=resolution)
    sink.write('test')

    month = datetime.today().strftime('%Y/%m')
    assert os.path.isdir(root)
    assert os.path.isfile('{}/{}.{}'.format(root, month, ext))

    del sink
    shutil.rmtree(root)


def test_datasink_hour_create_file():
    resolution = Datasink.HOUR
    sink = Datasink(root=root, ext=ext, resolution=resolution)
    sink.write('test')

    hour = datetime.now().strftime('%Y/%m/%d/%H')
    assert os.path.isdir(root)
    assert os.path.isfile('{}/{}.{}'.format(root, hour, ext))

    del sink
    shutil.rmtree(root)


def test_datasink_minute_create_file():
    resolution = Datasink.MINUTE
    sink = Datasink(root=root, ext=ext, resolution=resolution)
    sink.write('test')

    minute = datetime.now().strftime('%Y/%m/%d/%H/%M')
    assert os.path.isdir(root)
    assert os.path.isfile('{}/{}.{}'.format(root, minute, ext))

    del sink
    shutil.rmtree(root)


def test_no_overwrite_existing_file():
    resolution        = Datasink.MONTH
    existing_filename = '{}/{}.{}'.format(root, datetime.now().strftime('/%Y/%m'), ext)

    Path(existing_filename).parent.mkdir(mode=0o775, parents=True, exist_ok=True)
    open(existing_filename, mode='w').write('hello world')

    with pytest.raises(FileExistsError) as execinfo:
        sink = Datasink(root=root, ext=ext, resolution=resolution)
        sink.write()

    shutil.rmtree(root)


def test_custom_filename():
    def const_name():
        return 'custom'

    sink = Datasink(root=root, ext=ext, resolution=resolution, filename=const_name)
    sink.write('test')

    month = datetime.now().strftime('%Y/%m/')
    assert os.path.isfile('{}/{}{}.{}'.format(root, month, const_name(), ext))

    del sink
    shutil.rmtree(root)


def test_datasink_headers():
    header  = 'header'
    teststr = 'test'
    sink = Datasink(root=root, ext=ext, header=header, resolution=resolution)
    sink.write(teststr)
    sink._file

    with open(sink._file.name) as f:
        assert f.readline() == header + '\n'
        assert f.readline() == teststr + '\n'

    del sink
    shutil.rmtree(root)


def test_datasink_s3_buffer():
    root    = 'bucket/__test'
    backend = Datasink.S3
    teststr = 'hello world'

    sink = Datasink(root=root, ext=ext, backend=backend)
    sink.write(teststr)

    assert sink._file.getvalue() == teststr + '\n'
