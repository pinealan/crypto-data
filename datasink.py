import io
from pathlib import Path
from datetime import datetime


class Datasink:
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

    Cloud storage (AWS S3) is also supported. The first node in a path will be treated as the bucket
    name. i.e. path = bucket/data -> gets uploaded to s3://bucket

    .. note:
        Datasink does not create a S3 bucket for you.

    Args:
        path: Path to data directory. Absolute paths are prefered. Relative paths will be taken
            relative to script directory.
        ext: (optional) file extension for each file
        header: (optional) string to be put at the top of each file
        footer: (optional) string to be put at the bottom of each file
        timestamp: (optional) Toggle to prepend timestamp to the data records. Defaults to false.
        resolution: (optional) Defaults to Datasink.DAY
        backend: (optional) Defaults to OS file system. Valid values are 'os', 's3'

    @Future Creates a new data file according to one of the supported strategies.
    """

    # Resolutions
    MINUTE = 'min'
    HOUR   = 'hour'
    DAY    = 'day'
    MONTH  = 'month'

    # Backends
    OS = 'os'
    S3 = 's3'

    _dirfmt = {
        MINUTE : '%Y/%m/%d/%H/',
        HOUR   : '%Y/%m/%d/',
        DAY    : '%Y/%m/',
        MONTH  : '%Y/'
    }

    _filefmt = {
        MINUTE : '%M',
        HOUR   : '%H',
        DAY    : '%d',
        MONTH  : '%m'
    }

    _fullfmt = {
        MINUTE : '%Y%m%d-%H%M',
        HOUR   : '%Y%m%d-%H',
        DAY    : '%Y%m%d',
        MONTH  : '%Y%m'
    }

    def __init__(
            self,
            path,
            ext='',
            header=None,
            footer=None,
            fullname=False,
            resolution=DAY,
            backend=OS,
            backend_config={},
            filename=None):
        self._res = resolution
        self._ext = ext
        self._header = header
        self._footer = footer
        self._backend = backend

        # @Todo: Better variable naming
        self._fullname = fullname
        self._filename = filename

        if backend == self.OS:
            self._path = Path(path)
        elif backend == self.S3:
            # @Todo: Handle import exceptions
            import boto3
            pparts = Path(path).parts
            if 'aws_access_key_id' in backend_config and 'aws_secret_access_key' in backend_config:
                session = boto3.Session(
                        aws_access_key_id=backend_config['aws_access_key_id'],
                        aws_secret_access_key=backend_config['aws_secret_access_key'])
                self._bucket = session.resource('s3').Bucket(pparts[0])
            else:
                self._bucket = boto3.resource('s3').Bucket(pparts[0])
            self._path = Path('/'.join(pparts[1:]))

        self._newfile()
        self._addheader()

    def write(self, msg, add_time=False, delimiter=','):
        """Write entry to data sink."""
        if self._getfullpath() != self._filepath:
            self._nextfile()

        if add_time:
            msg = '{}{}{}'.format(datetime.now().timestamp(), delimiter, msg)

        self._file.write(msg + '\n')

    def _nextfile(self):
        self._addfooter()
        self.close()
        self._newfile()
        self._addheader()

    def close(self):
        """Close the datasink."""
        if self._backend == Datasink.OS:
            self._file.close()
        elif self._backend == Datasink.S3:
            self._obj.put(Body=bytes(self._file.getvalue(), 'utf8'))
            self._file.close()

    def _getfullpath(self):
        """Returns Path object of approperiate file."""
        time = datetime.now()
        if self._filename:
            subpath = time.strftime(self._dirfmt[self._res] + self._filefmt[self._res]) + '/'
            if isinstance(self._filename, str):
                 subpath += self._filename
            else:
                 subpath += self._filename()
        elif self._fullname:
            subpath = time.strftime(self._dirfmt[self._res] + self._fullfmt[self._res])
        else:
            subpath = time.strftime(self._dirfmt[self._res] + self._filefmt[self._res])

        return self._path / (subpath + self._ext)

    def _newfile(self):
        p = self._getfullpath()
        self._filepath = p

        if self._backend == Datasink.OS:

            # Prevent ovewriting existing files
            if p.exists():
                raise FileExistsError

            p.parent.mkdir(mode=0o775, parents=True, exist_ok=True)
            # line buffering, assuming each write will be a line
            self._file = p.open(mode='w', buffering=1)

        elif self._backend == Datasink.S3:
            self._file = io.StringIO()
            self._obj = self._bucket.Object(str(p))

    def _addheader(self):
        if self._header:
            self._file.write(self._header + '\n')

    def _addfooter(self):
        if self._footer:
            self._file._write(self._footer + '\n')
