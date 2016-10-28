import enum
import luigi
import logging
from slackclient import SlackClient

logger = logging.getLogger('luigi-interface')

class Jobs(enum.Enum):
    daily = 1
    weekly = 2


class State(luigi.Config):
    state_folder = luigi.Parameter()


class Act(luigi.Config):
    url = luigi.Parameter(default='world')
    username = luigi.Parameter(default='')
    password = luigi.Parameter(default='pass')


class H3(luigi.Config):
    host = luigi.Parameter()
    port = luigi.IntParameter()
    username = luigi.Parameter()
    password = luigi.Parameter()
    local_root_folder = luigi.Parameter()
    local_job_folder = luigi.Parameter()
    hdfs_root_folder = luigi.Parameter()


class Slack(luigi.Config):
    token = luigi.Parameter()

LOCAL_ROOT = H3().local_root_folder
LOCAL_JOBS_ROOT = H3().local_job_folder
WARC_ROOT = "%s/output/warcs" % H3().local_root_folder
VIRAL_ROOT = "%s/output/viral" % H3().local_root_folder
WREN_ROOT = "%s/output/wren" % H3().local_root_folder
IMAGE_ROOT = "%s/output/images" % H3().local_root_folder
LOG_ROOT = "%s/output/logs" % H3().local_root_folder
LOCAL_LOG_ROOT = "%s/output/logs" % H3().local_root_folder


def format_crawl_task(task):
    return '{} (launched {}-{}-{} {}:{})'.format(task.job.name, task.launch_id[:4],
                                                task.launch_id[4:6],task.launch_id[6:8],
                                                task.launch_id[8:10],task.launch_id[10:12])

def target_name(state_class, job, launch_id, state):
    return '{}-{}/{}/{}/{}.{}.{}.{}'.format(launch_id[:4],launch_id[4:6], job.name, launch_id, state_class, job.name, launch_id, state)


def vtarget(job, launch_id, state):
    return luigi.LocalTarget('{}/{}'.format(State().state_folder, target_name('07.verified', job, launch_id, state)))


def starget(job, launch_id, state):
    return luigi.LocalTarget('{}/{}'.format(State().state_folder, target_name('06.submitted', job, launch_id, state)))


def ptarget(job, launch_id, state):
    return luigi.LocalTarget('{}/{}'.format(State().state_folder, target_name('05.packaged', job, launch_id, state)))


def atarget(job, launch_id, state):
    return luigi.LocalTarget('{}/{}'.format(State().state_folder, target_name('04.assembled', job, launch_id, state)))


def otarget(job, launch_id, state):
    """
    Generate standardized state filename for job outputs:
    :param job:
    :param launch_id:
    :param state:
    :return:
    """
    return luigi.LocalTarget('{}/{}'.format(State().state_folder, target_name('03.outputs', job, launch_id, state)))


def ltarget(job, launch_id, state):
    return luigi.LocalTarget('{}/{}.zip'.format(State().state_folder, target_name('02.logs', job, launch_id, state)))


def jtarget(job, launch_id, state):
    return luigi.LocalTarget('{}/{}'.format(State().state_folder, target_name('01.jobs', job, launch_id, state)))


@luigi.Task.event_handler(luigi.Event.FAILURE)
def notify_failure(task, exception):
    """Will be called directly after a failed execution
       of `run` on any JobTask subclass
    """
    if Slack().token:
        sc = SlackClient(Slack().token)
        print(sc.api_call(
            "chat.postMessage", channel="#crawls", text="Job %s failed: :scream:\n_%s_" % (task, exception),
            username='crawljobbot'))  # , icon_emoji=':robot_face:'))
    else:
        logger.warning("No Slack auth token set, no message sent.")

