import os
import re
import enum
import json
import luigi
import datetime
import glob
import gzip
import time
from dateutil.parser import parse
import zipfile
import logging
from slackclient import SlackClient
from crawl.w3act.w3act import w3act
import crawl.h3.hapyx as hapyx
from crawl.w3act.job import W3actJob
from crawl.w3act.job import remove_action_files
from common import *
from crawl_job_tasks import CheckJobStopped


def get_state_suffix(stage):
    # Which suffix to use (i.e. are we packaging a checkpoint?):
    if stage == 'final':
        return ''
    else:
        return ".%s" % stage


class PackageLogs(luigi.Task):
    task_namespace = 'output'
    job = luigi.EnumParameter(enum=Jobs)
    launch_id = luigi.Parameter()
    stage = luigi.Parameter()

    def requires(self):
        if self.stage == 'final':
            return CheckJobStopped(self.job, self.launch_id)

    def output(self):
        return ltarget(self.job, self.launch_id, self.stage)

    def run(self):
        """Zips up all log/config. files and copies said archive to HDFS; finds the
        earliest timestamp in the logs."""
        # Set up the output, first making sure the full path exists:
        with self.output().open('w') as f:
            f.write('')
        self.output().remove()
        # What to remove from the paths:
        chop = len(str(LOCAL_ROOT))
        with zipfile.ZipFile(self.output().path, 'w', allowZip64=True) as zipout:
            for crawl_log in glob.glob("%s/%s/%s/crawl.log%s" % (
            LOCAL_LOG_ROOT, self.job.name, self.launch_id, get_state_suffix(self.stage))):
                logger.info("Found %s..." % os.path.basename(crawl_log))
                zipout.write(crawl_log, arcname=crawl_log[chop:])

            for log in glob.glob("%s/%s/%s/*-errors.log%s" % (
            LOCAL_LOG_ROOT, self.job.name, self.launch_id, get_state_suffix(self.stage))):
                logger.info("Found %s..." % os.path.basename(log))
                zipout.write(log, arcname=log[chop:])

            for txt in glob.glob("%s/%s/%s/*.txt" % (LOCAL_JOBS_ROOT, self.job.name, self.launch_id)):
                logger.info("Found %s..." % os.path.basename(txt))
                zipout.write(txt, arcname=txt[chop:])

            for txt in glob.glob("%s/%s/%s/*.json" % (LOCAL_JOBS_ROOT, self.job.name, self.launch_id)):
                logger.info("Found %s..." % os.path.basename(txt))
                zipout.write(txt, arcname=txt[chop:])

            cxml = "%s/%s/%s/crawler-beans.cxml" % (LOCAL_JOBS_ROOT, self.job.name, self.launch_id)
            if os.path.exists(cxml):
                logger.info("Found config...")
                zipout.write(cxml, arcname=cxml[chop:])
            else:
                logger.error("Cannot find config.")
                raise Exception("Cannot find config.")


class AssembleOutput(luigi.Task):
    task_namespace = 'output'
    job = luigi.EnumParameter(enum=Jobs)
    launch_id = luigi.Parameter()
    stage = luigi.Parameter(default='final')

    def requires(self):
        return PackageLogs(self.job, self.launch_id, self.stage)

    # TODO Move this into it's own job (atomicity):
    #    luigi.LocalTarget("%s/%s/%s/logs-%s.zip" % (self.LOCAL_LOG_ROOT, self.job.name, self.launch_id, self.stage))
    def output(self):
        return otarget(self.job, self.launch_id, self.stage)

    def run(self):
        logs = [self.get_crawl_log()]
        start_date = self.file_start_date(logs)
        # Find the WARCs referenced from the crawl log:
        (warcs, viral) = self.parse_crawl_log(logs)

        # TODO Get sha512 and ARK identifiers for WARCs now, and store in launch folder and thus the zip?
        # Loop over all the WARCs involved
        i = 0
        # self.hashes = {}
        for warc in warcs:
            # do some hard work here
            i += 1
            self.set_status_message("Progress: Hashing WARC %d of %s" % (i, len(warcs)))
            # hash_file = yield HashLocalFile(warc)
            # with hash_file.open('r') as f
            # Report on progress...
            self.set_status_message("Progress: Hashed WARC %d of %s" % (i, len(warcs)))

        # Bundle logs and configuration data into a zip and upload it to HDFS
        zips = [ self.input().path ]

        # FIXME Need to mint and add in ARKs at this point:

        # Output the job package summary:
        job_output = {
            'job_id': self.job.name,
            'launch_id': self.launch_id,
            'start_date': start_date,
            'warcs': warcs,
            'viral': viral,
            'logs': logs,
            'zips': zips,
        }

        with self.output().open('w') as f:
            f.write('{}'.format(json.dumps(job_output, indent=4)))

    def get_crawl_log(self):
        # First, parse the crawl log(s) and determine the WARC file names:
        logfilepath = "%s/%s/%s/crawl.log%s" % (
        LOCAL_LOG_ROOT, self.job.name, self.launch_id, get_state_suffix(self.stage))
        logger.info("Looking for crawl logs: %s" % logfilepath)
        if os.path.exists(logfilepath):
            logger.info("Found %s..." % os.path.basename(logfilepath))
            return logfilepath

    def file_start_date(self, logs):
        """Finds the earliest timestamp in a series of log files."""
        timestamps = []
        for log in logs:
            if log.endswith(".gz"):
                with gzip.open(log, "rb") as l:
                    fields = l.readline().split()
                    if len(fields) > 0:
                        timestamps.append(parse(fields[0]))
            else:
                with open(log, "rb") as l:
                    fields = l.readline().split()
                    if len(fields) > 0:
                        timestamps.append(parse(fields[0]))
        timestamps.sort()
        if len(timestamps) == 0:
            return None
        return timestamps[0].strftime("%Y-%m-%dT%H:%M:%SZ")

    def warc_file_path(self, warcfile):
        return "%s/%s/%s/%s" % (WARC_ROOT, self.job.name, self.launch_id, warcfile)

    def viral_file_path(self, warcfile):
        return "%s/%s/%s/%s" % (VIRAL_ROOT, self.job.name, self.launch_id, warcfile)

    def parse_crawl_log(self, logs):
        """
        Parses the crawl log to check the WARCs are present.
        :return:
        """
        warcfiles = set()
        with open(logs[0], 'r') as f:
            for line in f:
                parts = re.split(" +", line, maxsplit=11)
                # Skip failed downloads:
                if parts[1] == '-' or int(parts[1]) <= 0:
                    continue
                # Skip locally-resolved DNS records
                if parts[1] == "1001":
                    logger.debug("Skipping finding WARC for locally-defined hostname: %s" % parts[3])
                    continue
                # Attempt to parse JSON
                try:
                    (annotations, line_json) = re.split("{", parts[11], maxsplit=1)
                    line_json = "{%s" % line_json
                    # logger.debug("LOG JSON: %s" % line_json)
                    # logger.debug("LOG ANNOTATIONS: %s" % annotations)
                    jmd = json.loads(line_json)
                except Exception as e:
                    logger.info("LOG LINE: %s" % line)
                    logger.info("LOG LINE part[11]: %s" % parts[11])
                    logger.exception(e)
                    raise e
                if 'warcFilename' in jmd:
                    warcfiles.add(jmd['warcFilename'])
                elif 'warcPrefix' in jmd:
                    for wren in glob.glob(
                                    "%s/%s*.warc.gz*" % (WREN_ROOT, jmd['warcPrefix'])):
                        if wren.endswith('.open'):
                            wren = wren[:-5]
                        warcfiles.add(os.path.basename(wren))
                else:
                    raise Exception("No WARC file entry found for line: %s" % line)

        warcs = []
        viral = []
        for warcfile in warcfiles:
            if self._file_exists(self.viral_file_path(warcfile)):
                logger.info("Found Viral WARC %s" % self.viral_file_path(warcfile))
                viral.append(self.viral_file_path(warcfile))
            elif self._file_exists("%s/%s" % (WREN_ROOT, warcfile)):
                logger.info("Found WREN WARC %s" % warcfile)
                warcs.append("%s/%s" % (WREN_ROOT, warcfile))
            elif self._file_exists(self.warc_file_path(warcfile)):
                logger.info("Found WARC %s" % self.warc_file_path(warcfile))
                warcs.append(self.warc_file_path(warcfile))
            else:
                raise Exception("Cannot file warc file %s" % warcfile)

        return warcs, viral

    @staticmethod
    def _file_exists(path):
        """
        Checks whether the given file exists and has content - allowed to be '.open' at this point.

        :type path: str
        """
        if os.path.exists(path) and os.path.isfile(path) and os.path.getsize(path) > 0:
            return True
        elif os.path.exists("%s.open" % path) and os.path.isfile("%s.open" % path) and os.path.getsize(
                        "%s.open" % path) > 0:
            return True
        else:
            return False


class ProcessOutputs(luigi.Task):
    """
    """
    task_namespace = 'output'
    job = luigi.EnumParameter(enum=Jobs)
    launch_id = luigi.Parameter()

    def requires(self):
        # Look for checkpoints to package:
        for item in glob.glob("%s/%s/%s/crawl.log.cp*" % (LOG_ROOT, self.job.name, self.launch_id)):
            logger.info("ITEM %s" % item)
            yield AssembleOutput(self.job, self.launch_id, item[-22:])

        # Also look to package the final chunk
        yield AssembleOutput(self.job, self.launch_id, 'final')

    def output(self):
        return atarget(self.job, self.launch_id, "complete")

    def run(self):
        logger.info(self.launch_id)
        logger.info("OUTPUT: %s" % self.output().path)
        is_final = False
        outputs = []
        for input in self.input():
            if input.path.endswith(".final"):
                key = "final"
                is_final = True
            outputs.append(input.path)

        if is_final:
            # only report success if...
            with self.output().open('w') as f:
                f.write('{}'.format(json.dumps(outputs, indent=4)))
        else:
            yield CheckJobStopped(self.job, self.launch_id)


class ScanForOutputs(luigi.WrapperTask):
    """
    This task scans the output folder for jobs and instances of those jobs, looking for crawled content to process.
    """
    task_namespace = 'output'
    date_interval = luigi.DateIntervalParameter(default=[datetime.date.today(), datetime.date.today()])

    def requires(self):
        # Look for jobs that need to be processed:
        for date in self.date_interval:
            for job_item in glob.glob("%s/*" % H3().local_job_folder):
                job = Jobs[os.path.basename(job_item)]
                if os.path.isdir(job_item):
                    launch_glob = "%s/%s*" % (job_item, date.strftime('%Y%m%d'))
                    # self.set_status_message("Looking for job launch folders matching %s" % launch_glob)
                    for launch_item in glob.glob(launch_glob):
                        if os.path.isdir(launch_item):
                            launch = os.path.basename(launch_item)
                            # TODO Limit total number of processes?
                            yield ProcessOutputs(job, launch)


@AssembleOutput.event_handler(luigi.Event.SUCCESS)
def notify_success(task):
    """Will be called directly after a failed execution
       of `run` on any JobTask subclass
    """
    if Slack().token and task.stage == 'final':
        sc = SlackClient(Slack().token)
        print(sc.api_call(
            "chat.postMessage", channel="#crawls",
            text="Job %s finished and packaged successfully! :tada:"
                 % format_crawl_task(task), username='crawljobbot'))
        # , icon_emoji=':robot_face:'))
    else:
        logger.warning("No Slack auth token set, no message sent.")


if __name__ == '__main__':
    luigi.run(['output.ScanForOutputs', '--date-interval', '2016-10-23-2016-10-26'])  # , '--local-scheduler'])
# luigi.run(['crawl.job.StartJob', '--job', 'daily', '--local-scheduler'])



