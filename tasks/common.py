import os
import enum
import luigi
import logging
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
    hdfs_root_folder = luigi.Parameter()


class systems(luigi.Config):
    cdxserver = luigi.Parameter()
    wayback = luigi.Parameter()
    wrender = luigi.Parameter()
    amqp_host = luigi.Parameter()
    clamd_host = luigi.Parameter()
    clamd_port = luigi.Parameter()
    servers = luigi.Parameter()
    services = luigi.Parameter()


class slack(luigi.Config):
    token = luigi.Parameter()

LOCAL_ROOT = h3().local_root_folder
LOCAL_JOBS_ROOT = h3().local_job_folder
WARC_ROOT = "%s/output/warcs" % h3().local_root_folder
VIRAL_ROOT = "%s/output/viral" % h3().local_root_folder
WREN_ROOT = "%s/output/wren" % h3().local_root_folder
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


@luigi.Task.event_handler(luigi.Event.FAILURE)
def notify_failure(task, exception):
    """Will be called directly after a failed execution
       of `run` on any JobTask subclass
    """
    if slack().token:
        sc = SlackClient(slack().token)
        print(sc.api_call(
            "chat.postMessage", channel="#crawls", text="Job %s failed: :scream:\n_%s_" % (task, exception),
            username='crawljobbot'))  # , icon_emoji=':robot_face:'))
    else:
        logger.error("No Slack auth token set, no message sent.")
        logger.error(task)
        logger.exception(exception)

