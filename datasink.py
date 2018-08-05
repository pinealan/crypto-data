from pathlib import Path
from datetime import datetime, date


class DataSink:
    """Abstraction over file management for data lake curation.

    A directory is created to place all collected data. A date based directory
    hierachy will be used to organise the files. By default the file resolution
    is with day, i.e.

        dataset/
        |-- 2017/
        |   |-- 01/
        |   |   |-- 01.ext
        |   |   |-- 02.ext
        |   |
        |   |-- 02/
        |
        |-- 2018/

    Supported resolutions are [minute, hour, day, month].

    Attributes:
        resolution: The time resolution for new files to be created.

    Args:
        path: Path to data directory. Absolute paths are prefered. Relative paths will be taken
            relative to script directory.
        ext: (optional) file extension for each file
        header: (optional) string to be put at the top of each file
        footer: (optional) string to be put at the bottom of each file
        timestamp: (optional) Toggle to prepend timestamp to the data records. Defaults to false.
        resolution: (optional) Defaults to DataSink.DAY

    @Future Creates a new data file according to one of the supported strategies.
    """
    MINUTE = 'min'
    HOUR   = 'hour'
    DAY    = 'day'
    MONTH  = 'month'

    def __init__(
            self,
            path,
            ext='',
            header=None,
            footer=None,
            add_time=True,
            resolution=DAY):
        self.resolution = resolution
        self._path = Path(path)
        self._ext = ext
        self._header = header
        self._footer = footer
        self._add_time = add_time

        self._time = datetime.today()
        self._newfile()
        self._addheader()

    def write(self, msg):
        """Write entry to data sink."""
        if self._add_time:
            msg = '{},{}'.format(datetime.now().timestamp(), msg)
        msg += '\n'
        self.file().write(msg)

    def file(self):
        """Returns file object the approicate with approperiate path."""
        # @Todo: Support other resolutions, only DAY supported right now
        if datetime.today() != self._time:
            self._time = date.today()
            self._addfooter()
            self._newfile()
            self._addheader()
            return self._file
        else:
            return self._file

    def close(self):
        """Close the datasink."""
        self._file.close()

    def _newfile(self):
        if self.resolution == DataSink.DAY:
            p = self._path / self._time.strftime('%Y/%m')
            p.mkdir(mode=0o775, parents=True, exist_ok=True)
            p /= str(self._time.strftime('%d')) + self._ext
        elif self.resolution == DataSink.MONTH:
            p = self._path / self._time.strftime('%Y')
            p.mkdir(mode=0o775, parents=True, exist_ok=True)
            p /= str(self._time.strftime('%m')) + self._ext
        elif self.resolution == DataSink.HOUR:
            p = self._path / self._time.strftime('%Y/%m/%d')
            p.mkdir(mode=0o775, parents=True, exist_ok=True)
            p /= str(self._time.strftime('%H')) + self._ext
        elif self.resolution == DataSink.MINUTE:
            p = self._path / self._time.strftime('%Y/%m/%d/%H')
            p.mkdir(mode=0o775, parents=True, exist_ok=True)
            p /= str(self._time.strftime('%M')) + self._ext
        # line buffering, assuming each write will be a line
        self._file = p.open(mode='w', buffering=1)

    def _addheader(self):
        if self._header:
            self._file.write(self._header + '\n')

    def _addfooter(self):
        if self._footer:
            self._file._write(self._footer + '\n')
