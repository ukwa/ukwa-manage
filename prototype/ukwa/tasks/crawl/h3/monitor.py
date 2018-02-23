import os
import luigi
import shutil
import logging
from crawl_job_tasks import CheckJobStopped

logger = logging.getLogger(__name__)

class CloseOpenWarcFile(luigi.Task):
    """
    This task can close files that have been left .open, but it is not safe to tie this in usually as WARCs from
    warcprox do not get closed when the crawler runs a checkpoint.
    """
    task_namespace = 'file'
    host = luigi.Parameter()
    job = luigi.Parameter()
    launch_id = luigi.Parameter()
    path = luigi.Parameter()

    # Require that the job is stopped:
    def requires(self):
        return CheckJobStopped(self.job, self.launch_id)

    def output(self):
        return luigi.LocalTarget(self.path)

    def run(self):
        open_path = "%s.open" % self.path
        if os.path.isfile(open_path) and not os.path.isfile(self.path):
            logger.info("Found an open file that needs closing: %s " % open_path)
            shutil.move(open_path, self.path)


class ClosedWarcFile(luigi.ExternalTask):
    """
    An external process is responsible to closing open WARC files, so we declare it here.
    """
    task_namespace = 'file'
    host = luigi.Parameter()
    job = luigi.Parameter()
    launch_id = luigi.Parameter()
    path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.path)


