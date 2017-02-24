import os
import glob
import enum
import json
import luigi
import luigi.contrib.esindex
import string
import logging
import datetime
from slackclient import SlackClient

logger = logging.getLogger('luigi-interface')

HDFS_PREFIX = os.environ.get("HDFS_PREFIX", "")
WAYBACK_PREFIX = os.environ.get("WAYBACK_PREFIX", "http://localhost:9080/wayback")

LUIGI_STATE_FOLDER = os.environ['LUIGI_STATE_FOLDER']
ACT_URL = os.environ['ACT_URL']
ACT_USER = os.environ['ACT_USER']
ACT_PASSWORD = os.environ['ACT_PASSWORD']


def webhdfs():
    import hdfs
    return hdfs.InsecureClient(url=os.environ['WEBHDFS_PREFIX'], user=os.environ['WEBHDFS_USER'])


class state(luigi.Config):
    state_folder = os.environ.get('LUIGI_STATE_FOLDER', luigi.Parameter(default='/state'))


class act(luigi.Config):
    url = os.environ.get('W3ACT_URL', luigi.Parameter(default='http://w3act:9000/act'))
    username = os.environ.get('W3ACT_USER', luigi.Parameter(default='wa-sysadm@bl.uk'))
    password = os.environ.get('W3ACT_PW', luigi.Parameter(default='sysAdmin'))


class h3(luigi.Config):
    host = luigi.Parameter(default='ukwa-heritrix')
    port = luigi.IntParameter(default=8443)
    username = os.environ.get('HERITRIX_USER', luigi.Parameter(default='heritrix'))
    password = os.environ.get('HERITRIX_PASSWORD', luigi.Parameter(default='heritrix'))
    local_root_folder = luigi.Parameter(default='/heritrix')
    local_job_folder = luigi.Parameter(default='/jobs')
    local_wren_folder = luigi.Parameter(default='/heritrix/wren')
    hdfs_root_folder = os.environ.get('HDFS_PREFIX', luigi.Parameter('/1_data/pulse'))


class systems(luigi.Config):
    cdxserver = os.environ.get('CDXSERVER_URL', luigi.Parameter(default='http://cdxserver:8080/fc'))
    wayback = os.environ.get('WAYBACK_URL', luigi.Parameter(default='http://openwayback:8080/wayback'))
    wrender = os.environ.get('WRENDER_URL', luigi.Parameter(default='http://webrender:8010/render'))
    # Prefix for webhdfs queries, separate from general Luigi HDFS configuration.
    # e.g. http://localhost:50070/webhdfs/v1
    webhdfs = os.environ.get('WEBHDFS_PREFIX', luigi.Parameter(default='http://hadoop:50070/webhdfs/v1'))
    amqp_host = os.environ.get('AMQP_HOST', luigi.Parameter(default='amqp'))
    clamd_host = os.environ.get('CLAMD_HOST', luigi.Parameter(default='clamd'))
    clamd_port = os.environ.get('CLAMD_PORT', luigi.Parameter(default=3310))
    elasticsearch_host = os.environ.get('ELASTICSEARCH_HOST', luigi.Parameter(default='monitrix'))
    elasticsearch_port = os.environ.get('ELASTICSEARCH_PORT', luigi.Parameter(default=9200))
    elasticsearch_index_prefix = os.environ.get('ELASTICSEARCH_INDEX_PREFIX', luigi.Parameter(default='pulse'))
    servers = luigi.Parameter(default='/shepherd/tasks/servers.json')
    services = luigi.Parameter(default='/shepherd/tasks/services.json')


class slack(luigi.Config):
    token = os.environ.get('SLACK_TOKEN', luigi.Parameter())

LOCAL_ROOT = h3().local_root_folder
LOCAL_JOBS_ROOT = h3().local_job_folder
WARC_ROOT = "%s/output/warcs" % h3().local_root_folder
VIRAL_ROOT = "%s/output/viral" % h3().local_root_folder
IMAGE_ROOT = "%s/output/images" % h3().local_root_folder
LOG_ROOT = "%s/output/logs" % h3().local_root_folder
LOCAL_LOG_ROOT = "%s/output/logs" % h3().local_root_folder


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
    return luigi.LocalTarget('{}/{}'.format(state().state_folder, short_target_name('files/hash', job, launch_id,
                                                                              os.path.basename(file))))


def stats_target(job, launch_id, warc):
    return luigi.LocalTarget('{}/{}'.format(state().state_folder, short_target_name('warc/stats', job, launch_id,
                                                                              os.path.basename(warc))))


def dtarget(job, launch_id, status):
    return luigi.LocalTarget('{}/{}'.format(state().state_folder, target_name('logs/documents', job, launch_id, status)))


def vtarget(job, launch_id, status):
    return luigi.LocalTarget('{}/{}'.format(state().state_folder, target_name('07.verified', job, launch_id, status)))


def starget(job, launch_id, status):
    return luigi.LocalTarget('{}/{}'.format(state().state_folder, target_name('06.submitted', job, launch_id, status)))


def ptarget(job, launch_id, status):
    return luigi.LocalTarget('{}/{}'.format(state().state_folder, target_name('05.packaged', job, launch_id, status)))


def atarget(job, launch_id, status):
    return luigi.LocalTarget('{}/{}'.format(state().state_folder, target_name('04.assembled', job, launch_id, status)))


def otarget(job, launch_id, status):
    """
    Generate standardized state filename for job outputs:
    :param job:
    :param launch_id:
    :param state:
    :return:
    """
    return luigi.LocalTarget('{}/{}'.format(state().state_folder, target_name('03.outputs', job, launch_id, status)))


def ltarget(job, launch_id, status):
    return luigi.LocalTarget('{}/{}.zip'.format(state().state_folder, target_name('02.logs', job, launch_id, status)))


def jtarget(job, launch_id, status):
    return luigi.LocalTarget('{}/{}'.format(state().state_folder, target_name('01.jobs', job, launch_id, status)))


class ScanForLaunches(luigi.WrapperTask):
    """
    This task scans the output folder for jobs and instances of those jobs, looking for crawled content to process.

    Sub-class this and override the scan_job_launch method as needed.
    """
    task_namespace = 'scan'
    date_interval = luigi.DateIntervalParameter(
        default=[datetime.date.today() - datetime.timedelta(days=1), datetime.date.today()])
    timestamp = luigi.DateMinuteParameter(default=datetime.datetime.today())

    def requires(self):
        # Enumerate the jobs:
        for (job, launch) in self.enumerate_launches():
            logger.info("Processing %s/%s" % ( job, launch ))
            yield self.scan_job_launch(job, launch)

    def enumerate_launches(self):
        # Look for jobs that need to be processed:
        for date in self.date_interval:
            for job_item in glob.glob("%s/*" % h3().local_job_folder):
                job = Jobs[os.path.basename(job_item)]
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

    host = systems().elasticsearch_host
    port = systems().elasticsearch_port
    doc_type = 'default'
    #mapping = { "content": { "type": "text" } }
    purge_existing_index = False
    index =  "{}-{}".format(systems().elasticsearch_index_prefix,
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

    if os.environ.get("ELASTICSEARCH_HOST", None):
        doc = { 'content' : "Job %s failed: %s" % (task, exception) }
        source = 'luigi'
        esrm = RecordEvent("unknown_job", "unknown_launch_id", doc, source, "task-failure")
        esrm.run()
    else:
        logger.warning("No Elasticsearch host set, no failure message sent.")

    if os.environ.get("SLACK_TOKEN", None):
        sc = SlackClient(slack().token)
        print(sc.api_call(
            "chat.postMessage", channel="#crawls", text=":scream: Job _%s_ failed:\n> %s" % (task, exception),
            username='crawljobbot'))  # , icon_emoji=':robot_face:'))
    else:
        logger.error("No Slack auth token set, no message sent.")
        logger.error(task)
        logger.exception(exception)


@luigi.Task.event_handler(luigi.Event.SUCCESS)
def celebrate_any_success(task):
    """Will be called directly after a successful execution
       of `run` on any Task subclass (i.e. all luigi Tasks)
    """
    if os.environ.get("ELASTICSEARCH_HOST", None):
        doc = { 'content' : "Job %s succeeded." % task }
        source = 'luigi'
        esrm = RecordEvent("unknown_job", "unknown_launch_id", doc, source, "task-success")
        esrm.run()
    else:
        logger.warning("No Elasticsearch host set, no success message sent.")
