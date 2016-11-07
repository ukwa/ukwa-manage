import os
import glob
import enum
import luigi
import logging
import datetime
from slackclient import SlackClient

logger = logging.getLogger('luigi-interface')

class Jobs(enum.Enum):
    daily = 1
    weekly = 2


class state(luigi.Config):
    state_folder = luigi.Parameter()


class act(luigi.Config):
    url = luigi.Parameter(default='world')
    username = luigi.Parameter(default='')
    password = luigi.Parameter(default='pass')


class h3(luigi.Config):
    host = luigi.Parameter()
    port = luigi.IntParameter()
    username = luigi.Parameter()
    password = luigi.Parameter()
    local_root_folder = luigi.Parameter()
    local_job_folder = luigi.Parameter()
    local_wren_folder = luigi.Parameter()
    hdfs_root_folder = luigi.Parameter()


class systems(luigi.Config):
    cdxserver = luigi.Parameter()
    wayback = luigi.Parameter()
    wrender = luigi.Parameter()
    amqp_host = luigi.Parameter()
    clamd_host = luigi.Parameter()
    clamd_port = luigi.Parameter()
    elasticsearch_host = luigi.Parameter()
    elasticsearch_port = luigi.Parameter()
    elasticsearch_index_prefix = luigi.Parameter()
    servers = luigi.Parameter()
    services = luigi.Parameter()


class slack(luigi.Config):
    token = luigi.Parameter()

LOCAL_ROOT = h3().local_root_folder
LOCAL_JOBS_ROOT = h3().local_job_folder
WARC_ROOT = "%s/output/warcs" % h3().local_root_folder
VIRAL_ROOT = "%s/output/viral" % h3().local_root_folder
IMAGE_ROOT = "%s/output/images" % h3().local_root_folder
LOG_ROOT = "%s/output/logs" % h3().local_root_folder
LOCAL_LOG_ROOT = "%s/output/logs" % h3().local_root_folder


def format_crawl_task(task):
    return '{} (launched {}-{}-{} {}:{})'.format(task.job.name, task.launch_id[:4],
                                                task.launch_id[4:6],task.launch_id[6:8],
                                                task.launch_id[8:10],task.launch_id[10:12])


def target_name(state_class, job, launch_id, status):
    return '{}-{}/{}/{}/{}.{}.{}.{}'.format(launch_id[:4],launch_id[4:6], job.name, launch_id, state_class, job.name, launch_id, status)


def short_target_name(state_class, job, launch_id, tail):
    return '{}-{}/{}/{}/{}.{}'.format(launch_id[:4],launch_id[4:6], job.name, launch_id, state_class, tail)


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


class ScanForLaunches(luigi.Task):
    """
    This task scans the output folder for jobs and instances of those jobs, looking for crawled content to process.

    Sub-class this and override the scan_job_launch method as needed.
    """
    task_namespace = 'scan'
    date_interval = luigi.DateIntervalParameter(
        default=[datetime.date.today() - datetime.timedelta(days=1), datetime.date.today()])
    timestamp = luigi.DateMinuteParameter(default=datetime.datetime.today())

    def output(self):
        return luigi.LocalTarget('{}/{}/scans/{}.{}.{}'.format(
            state().state_folder, self.timestamp.strftime('%Y-%m'),
            self.scan_name, self.date_interval, self.timestamp.isoformat()))

    def run(self):
        # Look for jobs that need to be processed:
        for date in self.date_interval:
            for job_item in glob.glob("%s/*" % h3().local_job_folder):
                job = Jobs[os.path.basename(job_item)]
                if os.path.isdir(job_item):
                    launch_glob = "%s/%s*" % (job_item, date.strftime('%Y%m%d'))
                    # self.set_status_message("Looking for job launch folders matching %s" % launch_glob)
                    for launch_item in glob.glob(launch_glob):
                        if os.path.isdir(launch_item):
                            launch = os.path.basename(launch_item)
                            # TODO Limit total number of processes?
                            yield self.scan_job_launch(job, launch)
        # Log that we ran okay:
        with self.output().open('w') as out_file:
            #out_file.write('{}'.format(json.dumps(stats, indent=4)))
            out_file.write('{}'.format(self.date_interval))

    def scan_job_launch(self, job, launch):
        pass


@luigi.Task.event_handler(luigi.Event.FAILURE)
def notify_failure(task, exception):
    """Will be called directly after a failed execution
       of `run` on any JobTask subclass
    """
    if slack().token:
        sc = SlackClient(slack().token)
        print(sc.api_call(
            "chat.postMessage", channel="#crawls", text="Job _%s_ failed: :scream:\n> %s" % (task, exception),
            username='crawljobbot'))  # , icon_emoji=':robot_face:'))
    else:
        logger.error("No Slack auth token set, no message sent.")
        logger.error(task)
        logger.exception(exception)

