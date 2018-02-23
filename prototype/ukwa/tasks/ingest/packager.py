import os
import re
import enum
import json
import luigi
import datetime
import glob
from tasks.settings import logger, LUIGI_STATE_FOLDER

class ScanForPackages(luigi.WrapperTask):
    """
    This task scans the output folder for jobs and instances of those jobs, looking for crawled content to process.
    """
    task_namespace = 'output'
    date_interval = luigi.DateIntervalParameter(
        default=[datetime.date.today() - datetime.timedelta(days=1), datetime.date.today()])

    def requires(self):
        # Look for jobs that need to be processed:
        for date in self.date_interval:
            for job_item in glob.glob("%s/*/*" % LUIGI_STATE_FOLDER):
                job = os.path.basename(job_item)
                if os.path.isdir(job_item):
                    launch_glob = "%s/%s*" % (job_item, date.strftime('%Y%m%d'))
                    # self.set_status_message("Looking for job launch folders matching %s" % launch_glob)
                    for launch_item in glob.glob(launch_glob):
                        if os.path.isdir(launch_item):
                            launch = os.path.basename(launch_item)
                            # TODO Limit total number of processes?
                            logger.info("ScanForPackages - looking at %s %s" % (job, launch_item))
                            yield ProcessPackages(job, launch, launch_item)



if __name__ == '__main__':
    luigi.run(['output.ScanForPackages', '--date-interval', '2016-10-22-2016-10-26'])  # , '--local-scheduler'])
# luigi.run(['crawl.job.StartJob', '--job', 'daily', '--local-scheduler'])



