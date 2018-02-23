import os
import zlib
import hdfs
import gzip
import logging
import tempfile
import luigi
import luigi.contrib.hdfs
import luigi.contrib.hdfs.format

logger = logging.getLogger(__name__)


def webhdfs():
    client = hdfs.InsecureClient(url=os.environ.get('WEBHDFS_URL'), user=os.environ.get('WEBHDFS_USER'))
    return client


class WebHdfsPlainFormat(luigi.format.Format):
    """
    This custom format can be used to ensure reads and writes to HDFS go over WebHDFS.

            # HdfsTarget cannot write to HDFS when using the HDFS client!
            # And WebHdfsTarget cannot be read!
    """

    input = 'bytes'
    output = 'hdfs'

    def __init__(self, use_gzip=False):
        self._use_gzip = use_gzip

    def hdfs_writer(self, path):
        return self.pipe_writer(path)

    def hdfs_reader(self, path):
        return self.pipe_reader(path)

    def pipe_reader(self, path):
        return WebHdfsReadPipe(path, self._use_gzip)

    def pipe_writer(self, output_pipe):
        return WebHdfsAtomicWritePipe(output_pipe, self._use_gzip)


class WebHdfsReadPipe(object):

    def __init__(self, path, use_gzip=False, fs=None):
        """
        Initializes a WebHdfsReadPipe instance.

        :param path: a path
        """
        self._use_gzip = use_gzip
        self._path = path

        if self._use_gzip:
            if not self._path.endswith('.gz'):
                raise Exception("Gzipped files should end with '.gz' and '%s' does not!" % self._path)
        else:
            if self._path.endswith('.gz'):
                raise Exception("Only gzipped files should end with '.gz' and '%s' does!" % self._path)

        self._fs = fs or luigi.contrib.hdfs.hdfs_clients.hdfs_webhdfs_client.WebHdfsClient()

    def _finish(self):
        pass

    def close(self):
        self._finish()

    def __del__(self):
        self._finish()

    def __enter__(self):
        return self

    def _abort(self):
        """
        Call _finish, but eat the exception (if any).
        """
        try:
            self._finish()
        except KeyboardInterrupt:
            raise
        except BaseException:
            pass

    def __exit__(self, type, value, traceback):
        if type:
            self._abort()
        else:
            self._finish()

    def __iter__(self):
        if self._use_gzip:
            d = zlib.decompressobj(16 + zlib.MAX_WBITS)
            last_line = ""
            try:
                with self._fs.client.read(self._path) as reader:
                    for gzchunk in reader:
                        chunk = "%s%s" % (last_line, d.decompress(gzchunk))
                        chunk_by_line = chunk.split('\n')
                        last_line = chunk_by_line.pop()
                        for line in chunk_by_line:
                            yield line
            except StopIteration:  # the other end of the pipe is empty
                yield last_line
                raise StopIteration

        else:
            with self._fs.client.read(self._path) as reader:
                for line in reader:
                    yield line
        self._finish()

    def readable(self):
        return True

    def writable(self):
        return False

    def seekable(self):
        return False


class WebHdfsAtomicWritePipe(object):

    def __init__(self, path, use_gzip=False, fs=None):
        """
        Initializes a WebHdfsReadPipe instance.

        :param path: a path
        """
        self._path = path
        logger.error("PATH %s" % self._path)
        self._use_gzip = use_gzip
        self._fs = fs or luigi.contrib.hdfs.hdfs_clients.hdfs_webhdfs_client.WebHdfsClient()

        # Create the temporary file name:
        self._tmp_path = "%s.temp" % self._path

        # Now upload the file, allowing overwrites as this is a temporary file and
        # simultanous updates should not be possible:
        logger.info("Will upload as %s" % self._tmp_path)

        # Check if the destination file exists and raise an exception if so:
        if self._fs.exists(self._path):
            raise Exception("Path %s already exists! This should never happen!" % self._path)

        if self._use_gzip:
            if not self._path.endswith('.gz'):
                raise Exception("Gzipped files should end with '.gz' and '%s' does not!" % self._path)
            self._temp = tempfile.NamedTemporaryFile(delete=False)
            self._writer = gzip.GzipFile(fileobj=self._temp, mode='wb')
        else:
            if self._path.endswith('.gz'):
                raise Exception("Only gzipped files should end with '.gz' and '%s' does!" % self._path)
            self._writer = self._fs.client.write(self._tmp_path, overwrite=True)

        self.closed = False

    def write(self, *args, **kwargs):
        self._writer.write(*args, **kwargs)

    def writeLine(self, line):
        assert '\n' not in line
        self.write(line + '\n')

    def _finish(self):
        pass

    def __del__(self):
        if not self.closed:
            self.abort()

    def __exit__(self, type, value, traceback):
        """
        Shovel file up to HDFS if we've been writing to a local .gz
        """
        self._writer.__exit__(type, value, traceback)

        if self._use_gzip:
            self._temp.flush()
            name = self._temp.name
            read_temp = open(name)
            self._fs.client.write(self._tmp_path, data=read_temp, overwrite=True)
            read_temp.close()
            os.remove(name)

        # Move the uploaded file into the right place:
        self._fs.client.rename(self._tmp_path, self._path)

    def __enter__(self):
        self._writer.__enter__()
        return self

    def close(self):
        self._finish()

    def abort(self):
        self._finish()

    def readable(self):
        return False

    def writable(self):
        return True

    def seekable(self):
        return False

