import io
import logging
from pathlib import Path
from datetime import datetime


logger = logging.getLogger(__name__)


class Datasink:
    """Data lake abstaction with time based file management.

    A root directory is created to place all collected data. A date denominated
    hierachical structure will be used to organise the data files. On write to
    an instance of stink, a new data file will be automatically created when
    appropriate. By default the file resolution is days, i.e.

        dataset
        |- 2017
        |  |- 01
        |  |  |- 01.csv
        |  |  |- 02.csv
        |  |
        |  |- 02/
        |
        |- 2018/

    Supported resolutions are [minute, hour, day, month].

    Cloud storage (AWS S3) is also supported. The first node in a path will be treated as the bucket
    name. i.e. path = bucket/data -> gets uploaded to s3://bucket

    .. note:
        Datasink does not create a S3 bucket for you.

    Args:
        path: Path to data directory. Absolute paths are prefered. Relative paths will be taken
            relative to script directory.
        ext: (optional) file extension for each file
        header: (optional) String to be put at the top of each file
        footer: (optional) String to be put at the bottom of each file
        fullname: (optional) Boolean for toggling full timestamp as filenames.
        resolution: (optional) Defaults to Datasink.DAY
        backend: (optional) Defaults to OS file system. Valid values are 'os', 's3'
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
            root,
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
            self._root = Path(root)
        elif backend == self.S3:
            # @Todo: Handle import exceptions
            pparts = Path(root).parts
            self._bucket = self._get_s3_bucket(pparts[0], backend_config)
            self._root   = Path('/'.join(pparts[1:]))

        self._newfile()
        self._addheader()

    def write(self, msg, add_time=False, delimiter=','):
        """Write entry to data sink."""
        if self._getfullpath() != self._filepath:
            self._nextfile()

        if add_time:
            msg = '{}{}{}'.format(datetime.now().timestamp(), delimiter, msg)

        logger.debug('Writing data entry to {}.'.format(self._filepath))

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

        return self._root / '{}.{}'.format(subpath, self._ext)

    def _newfile(self):
        p = self._getfullpath()
        self._filepath = p

        if self._backend == Datasink.OS:

            # Prevent ovewriting existing files
            if p.exists():
                logger.warning('File {} exists. Refusing to overwrite'.format(p))
                raise FileExistsError

            p.parent.mkdir(mode=0o775, parents=True, exist_ok=True)
            # line buffering, assuming each write will be a line
            self._file = p.open(mode='w', buffering=1)
            logger.debug('Create local file {}'.format(p))

        elif self._backend == Datasink.S3:
            self._file = io.StringIO()
            self._obj = self._bucket.Object(str(p))
            logger.debug('Create S3 object {}'.format(p))

    def _addheader(self):
        if self._header:
            self._file.write(self._header + '\n')

    def _addfooter(self):
        if self._footer:
            self._file._write(self._footer + '\n')

    def _get_s3_bucket(self, bucket, config):
        import boto3

        if 'aws_access_key_id' in config and 'aws_secret_access_key' in config:
            session = boto3.Session(
                aws_access_key_id=config['aws_access_key_id'],
                aws_secret_access_key=config['aws_secret_access_key']
            )
            return session.resource('s3').Bucket(bucket)
        else:
            return boto3.resource('s3').Bucket(bucket)


def log_to_stdout(level=logging.WARNING, formatter=None):
    handler = logging.StreamHandler()
    handler.setLevel(level)
    if formatter:
        handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.setLevel(level)
