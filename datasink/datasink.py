import io
import os
import logging
from pathlib import Path
from datetime import datetime, timezone


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

    Cloud storage (AWS S3) is also supported. The first node in a path will be
    treated as the bucket name.
    i.e. path = bucket/data -> gets uploaded to s3://bucket

    .. note:
        Datasink does not create a S3 bucket for you.

    Args
    ----
    root : str
        Path to data directory. Absolute paths are prefered. Relative paths will
        be taken relative to script directory.
    ext : str
        File extension for each file
    header : str
        String to be put at the top of each file
    footer : str
        String to be put at the bottom of each file
    name : int
        Choice of file naming modes:
        0) smallest denomination from provided resolution [default]
        1) unix timestamp
        2) full ISO datetime
    resolution : str
        Defaults to Datasink.DAY
    backend : str
        Defaults to OS file system. Valid values are 'os', 's3'
    backend : dict
        Custom configs to be passed to specified backend

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
        MINUTE : '%Y%m%dT%H%M',
        HOUR   : '%Y%m%dT%H',
        DAY    : '%Y%m%d',
        MONTH  : '%Y%m'
    }

    def __init__(
            self,
            root,
            ext='csv',
            header=None,
            footer=None,
            namemode=0,
            resolution=DAY,
            backend=OS,
            backend_config={}
        ):
        self._res = resolution
        self._ext = ext
        self._header = header
        self._footer = footer
        self._mode = namemode
        self._backend = backend

        if backend == self.OS:
            self._root = Path(root)
        elif backend == self.S3:
            # @Todo: Handle import exceptions
            pparts = Path(root).parts
            self._bucket = self._get_s3_bucket(pparts[0], backend_config)
            self._root   = Path('/'.join(pparts[1:]))

        self._newfile()
        self._addheader()

    def write(self, msg):
        """Write entry to data sink."""
        if self._getfullpath() != self._filepath:
            self._nextfile()

        logger.debug('Writing data entry to {}.'.format(self._filepath))

        self._file.write(msg + '\n')

    def close(self):
        """Close the datasink."""

        # Close local file
        if self._backend == Datasink.OS:
            self._file.close()
            logger.info('Close local file')

        # Write buffer to S3 object
        elif self._backend == Datasink.S3:
            self._obj.put(Body=bytes(self._file.getvalue(), 'utf8'))
            self._file.close()
            logger.info('Sent file to AWS S3')

    def _nextfile(self):
        self._addfooter()
        self.close()
        logger.info('Rotating to next file')
        self._newfile()
        self._addheader()

    def _getfullpath(self):
        """Return approperiate file Path determined by current time."""

        time = datetime.now()

        # root/2018/11/30/08.csv
        if self._mode == 0:
            subpath = time.strftime(self._dirfmt[self._res] \
                    + self._filefmt[self._res])

        # root/2018/11/30/1501231921.csv
        elif self._mode == 1:
            subpath = time.strftime(self._dirfmt[self._res] \
                    + self._fullfmt[self._res] + '/' ) \
                    + str(int(time.replace(tzinfo=timezone.utc).timestamp()))

        # root/2018/11/30/20181130.csv
        elif self._mode == 2:
            subpath = time.strftime(self._dirfmt[self._res] \
                    + self._fullfmt[self._res])

        else:
            raise ValueError('Unrecognized file naming operation')

        return self._root / '{}.{}'.format(subpath, self._ext)

    def _newfile(self):
        """Rotate to a new IO object as the sink buffer."""
        self._filepath = p = self._getfullpath()

        # Open new local file
        if self._backend == Datasink.OS:
            # Prevent ovewriting existing files
            if p.exists():
                logger.warning('File {} exists. Refusing to overwrite'.format(p))

            p.parent.mkdir(mode=0o775, parents=True, exist_ok=True)

            # line buffering, assuming each write will be a line
            self._file = p.open(mode='w', buffering=1)
            logger.info('Create local file {}'.format(p))

        # Create new buffer for S3 object
        elif self._backend == Datasink.S3:
            self._file = io.StringIO()
            self._obj = self._bucket.Object(str(p))
            logger.info('Create IO object {} as buffer for S3'.format(p))

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
        elif 'AWS_ACCESS_KEY_ID' in os.environ and 'AWS_SECRET_ACCESS_KEY' in os.environ:
            session = boto3.Session(
                aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY']
            )
            return session.resource('s3').Bucket(bucket)
        else:
            return boto3.resource('s3').Bucket(bucket)


def stdout_logger(level=logging.INFO, formatter=None):
    handler = logging.StreamHandler()
    handler.setLevel(level)
    if formatter:
        handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.setLevel(level)
    return logger
