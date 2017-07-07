import os
import glob
import hdfs
import string
import logging
import datetime
import luigi
import luigi.date_interval
import luigi.contrib.esindex
import luigi.contrib.hdfs
import settings

logger = logging.getLogger('luigi-interface')


LUIGI_STATE_FOLDER = settings.state().folder


def state_file(date, tag, suffix, on_hdfs=False):
    path = os.path.join( LUIGI_STATE_FOLDER,
                         date.strftime("%Y-%m"),
                         tag,
                         '%s-%s' % (date.strftime("%Y-%m-%d"), suffix))
    if on_hdfs:
        return luigi.contrib.hdfs.HdfsTarget(path=path)
    else:
        return luigi.LocalTarget(path=path)


def webhdfs():
    client = hdfs.InsecureClient(url=os.environ['WEBHDFS_PREFIX'], user=os.environ['WEBHDFS_USER'])
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
        if isinstance(self.event, luigi.parameter.FrozenOrderedDict):
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
