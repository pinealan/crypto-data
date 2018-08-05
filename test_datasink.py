import os
import shutil
import datetime as dt

from datasink import DataSink


def test_datasink_day_create_file():
    path       = './__test'
    ext        = '.csv'
    resolution = DataSink.DAY
    sink = DataSink(path=path, ext=ext, resolution=resolution)
    sink.write('test')

    today = dt.date.today().strftime('%Y/%m/%d')
    assert os.path.isdir(path)
    assert os.path.isfile('{}/{}{}'.format(path, today, ext))

    del sink
    shutil.rmtree(path)
