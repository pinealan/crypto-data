import os
import shutil
import datetime as dt

from datasink import Datasink


def test_datasink_day_create_file():
    path       = './__test'
    ext        = '.csv'
    resolution = Datasink.DAY
    sink = Datasink(path=path, ext=ext, resolution=resolution)
    sink.write('test')

    today = dt.date.today().strftime('%Y/%m/%d')
    assert os.path.isdir(path)
    assert os.path.isfile('{}/{}{}'.format(path, today, ext))

    del sink
    shutil.rmtree(path)


def test_datasink_month_create_file():
    path       = './__test'
    ext        = '.csv'
    resolution = Datasink.MONTH
    sink = Datasink(path=path, ext=ext, resolution=resolution)
    sink.write('test')

    month = dt.date.today().strftime('%Y/%m')
    assert os.path.isdir(path)
    assert os.path.isfile('{}/{}{}'.format(path, month, ext))

    del sink
    shutil.rmtree(path)


def test_datasink_hour_create_file():
    path       = './__test'
    ext        = '.csv'
    resolution = Datasink.HOUR
    sink = Datasink(path=path, ext=ext, resolution=resolution)
    sink.write('test')

    hour = dt.datetime.now().strftime('%Y/%m/%d/%H')
    assert os.path.isdir(path)
    assert os.path.isfile('{}/{}{}'.format(path, hour, ext))

    del sink
    shutil.rmtree(path)


def test_datasink_minute_create_file():
    path       = './__test'
    ext        = '.csv'
    resolution = Datasink.MINUTE
    sink = Datasink(path=path, ext=ext, resolution=resolution)
    sink.write('test')

    minute = dt.datetime.now().strftime('%Y/%m/%d/%H/%M')
    assert os.path.isdir(path)
    assert os.path.isfile('{}/{}{}'.format(path, minute, ext))

    del sink
    shutil.rmtree(path)


def test_datasink_s3_create():
    path    = 'bucket/__test'
    ext     = '.csv'
    backend = Datasink.S3

    sink = Datasink(path=path, ext=ext, backend=backend)
    sink.write('hello world')
    # nothing should have happened till this point
