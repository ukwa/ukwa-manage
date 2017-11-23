import os
import posixpath
import glob
import zlib
import hdfs
import gzip
import string
import logging
import datetime
import tempfile
import luigi
import luigi.date_interval
import luigi.contrib.esindex
import luigi.contrib.hdfs
import luigi.contrib.hdfs.format
import settings

logger = logging.getLogger(__name__)

LUIGI_STATE_FOLDER = settings.state().folder
LUIGI_HDFS_STATE_FOLDER = settings.state.hdfs_folder


def state_file(date, tag, suffix, on_hdfs=False, use_gzip=False, use_webhdfs=True):
    # Set up the state folder:
    state_folder = settings.state().folder
    pather = os.path
    if on_hdfs:
        pather = posixpath
        state_folder = settings.state().hdfs_folder

    # build the full path:
    if date:
        full_path = pather.join( str(state_folder),
                         date.strftime("%Y-%m"),
                         tag,
                         '%s-%s' % (date.strftime("%Y-%m-%d"), suffix))
    else:
        full_path = pather.join( str(state_folder), tag, suffix)

    if on_hdfs:
        if use_webhdfs:
            return luigi.contrib.hdfs.HdfsTarget(path=full_path, format=WebHdfsPlainFormat(use_gzip=use_gzip))
        else:
            return luigi.contrib.hdfs.HdfsTarget(path=full_path, format=luigi.contrib.hdfs.PlainFormat())
    else:
        return luigi.LocalTarget(path=full_path)


def webhdfs():
    client = hdfs.InsecureClient(url=settings.systems().webhdfs, user=settings.systems().webhdfs_user)
    return client


def check_hash(path, file_hash):
    logger.debug("Checking file %s hash %s" % (path, file_hash))
    if len(file_hash) != 128:
        raise Exception("%s hash not 128 character length [%s]" % (path, len(file_hash)))
    if not all(c in string.hexdigits for c in file_hash):
        raise Exception("%s hash not all hex [%s]" % (path, file_hash))


def format_crawl_task(task):
    return '{} (launched {}-{}-{} {}:{})'.format(task.job, task.launch_id[:4],
                                                task.launch_id[4:6],task.launch_id[6:8],
                                                task.launch_id[8:10],task.launch_id[10:12])


def target_name(state_class, job, launch_id, status):
    return '{}-{}/{}/{}/{}.{}.{}.{}'.format(launch_id[:4],launch_id[4:6], job, launch_id, state_class, job, launch_id, status)


def short_target_name(state_class, job, launch_id, tail):
    return '{}-{}/{}/{}/{}.{}'.format(launch_id[:4],launch_id[4:6], job, launch_id, state_class, tail)


def hash_target(job, launch_id, file):
    return luigi.LocalTarget('{}/{}'.format(settings.state().state_folder, short_target_name('files/hash', job, launch_id,
                                                                              os.path.basename(file))))


def stats_target(job, launch_id, warc):
    return luigi.LocalTarget('{}/{}'.format(settings.state().state_folder, short_target_name('warc/stats', job, launch_id,
                                                                              os.path.basename(warc))))


def dtarget(job, launch_id, status):
    return luigi.LocalTarget('{}/{}'.format(settings.state().state_folder, target_name('logs/documents', job, launch_id, status)))


def vtarget(job, launch_id, status):
    return luigi.LocalTarget('{}/{}'.format(settings.state().state_folder, target_name('07.verified', job, launch_id, status)))


def starget(job, launch_id, status):
    return luigi.LocalTarget('{}/{}'.format(settings.state().state_folder, target_name('06.submitted', job, launch_id, status)))


def ptarget(job, launch_id, status):
    return luigi.LocalTarget('{}/{}'.format(settings.state().state_folder, target_name('05.packaged', job, launch_id, status)))


def atarget(job, launch_id, status):
    return luigi.LocalTarget('{}/{}'.format(settings.state().state_folder, target_name('04.assembled', job, launch_id, status)))


def otarget(job, launch_id, status):
    """
    Generate standardized state filename for job outputs:
    :param job:
    :param launch_id:
    :param state:
    :return:
    """
    return luigi.LocalTarget('{}/{}'.format(settings.state().state_folder, target_name('03.outputs', job, launch_id, status)))


def ltarget(job, launch_id, status):
    return luigi.LocalTarget('{}/{}.zip'.format(settings.state().state_folder, target_name('02.logs', job, launch_id, status)))


def jtarget(job, launch_id, status):
    return luigi.LocalTarget('{}/{}'.format(settings.state().state_folder, target_name('01.jobs', job, launch_id, status)))


def get_large_interval():
    """
    This sets up a default, large window for operations.

    :return:
    """
    interval = luigi.date_interval.Custom(
        datetime.date.today() - datetime.timedelta(weeks=52),
        datetime.date.today() + datetime.timedelta(days=1))
    return interval


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


class ScanForLaunches(luigi.WrapperTask):
    """
    This task scans the output folder for jobs and instances of those jobs, looking for crawled content to process.

    Sub-class this and override the scan_job_launch method as needed.
    """
    task_namespace = 'scan'
    date_interval = luigi.DateIntervalParameter(default=get_large_interval())
    timestamp = luigi.DateMinuteParameter(default=datetime.datetime.today())

    def requires(self):
        # Enumerate the jobs:
        for (job, launch) in self.enumerate_launches():
            logger.info("Processing %s/%s" % ( job, launch ))
            yield self.scan_job_launch(job, launch)

    def enumerate_launches(self):
        # Look for jobs that need to be processed:
        for date in self.date_interval:
            logger.info("Looking at date %s" % date)
            for job_item in glob.glob("%s/*" % settings.h3().local_job_folder):
                job = os.path.basename(job_item)
                if os.path.isdir(job_item):
                    launch_glob = "%s/%s*" % (job_item, date.strftime('%Y%m%d'))
                    logger.info("Looking for job launch folders matching %s" % launch_glob)
                    for launch_item in glob.glob(launch_glob):
                        logger.info("Found %s" % launch_item)
                        if os.path.isdir(launch_item):
                            launch = os.path.basename(launch_item)
                            yield (job, launch)


class RecordEvent(luigi.contrib.esindex.CopyToIndex):
    """
    Post this event to Monitrix, i.e. push into an appropriate Elasticsearch index.
    """
    task_namespace = 'doc'
    job = luigi.Parameter()
    launch_id = luigi.Parameter()
    event = luigi.DictParameter()
    source = luigi.Parameter()
    event_type = luigi.Parameter()

    host = settings.systems().elasticsearch_host
    port = settings.systems().elasticsearch_port
    doc_type = 'default'
    #mapping = { "content": { "type": "text" } }
    purge_existing_index = False
    index =  "{}-{}".format(settings.systems().elasticsearch_index_prefix,
                             datetime.datetime.now().strftime('%Y-%m-%d'))

    def docs(self):
        if isinstance(self.event, luigi.parameter._FrozenOrderedDict):
            doc = self.event.get_wrapped()
        else:
            doc = {}
        # Add more default/standard fields:
        doc['timestamp'] = datetime.datetime.now().isoformat()
        doc['job'] = self.job
        doc['launch_id'] = self.launch_id
        doc['source'] = self.source
        doc['event_type'] = self.event_type
        return [doc]


@luigi.Task.event_handler(luigi.Event.FAILURE)
def notify_any_failure(task, exception):
    """Will be called directly after a failed execution
       of `run` on any JobTask subclass
    """

    if settings.systems().elasticsearch_host:
        doc = { 'content' : "Job %s failed: %s" % (task, exception) }
        source = 'luigi'
        esrm = RecordEvent("unknown_job", "unknown_launch_id", doc, source, "task-failure")
        esrm.run()
    else:
        logger.warning("No Elasticsearch host set, no failure message sent.")


@luigi.Task.event_handler(luigi.Event.SUCCESS)
def celebrate_any_success(task):
    """Will be called directly after a successful execution
       of `run` on any Task subclass (i.e. all luigi Tasks)
    """
    if settings.systems().elasticsearch_host:
        doc = { 'content' : "Job %s succeeded." % task }
        source = 'luigi'
        esrm = RecordEvent("unknown_job", "unknown_launch_id", doc, source, "task-success")
        esrm.run()
    else:
        logger.warning("No Elasticsearch host set, no success message sent.")
