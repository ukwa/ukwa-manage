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
from move_to_hdfs import CalculateLocalHash, get_hdfs_target


def get_stage_suffix(stage):
    """
    Which suffix to use (i.e. are we packaging a checkpoint?) - maps a stage to a suffix.
    :param stage:
    :return:
    """
    if stage == 'final':
        return ''
    else:
        return ".%s" % stage


class PackageLogs(luigi.Task):
    """
    For a given job launch and stage, package up the logs and config in a ZIP to go inside the main crawl SIP.
    """
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
            LOCAL_LOG_ROOT, self.job.name, self.launch_id, get_stage_suffix(self.stage))):
                logger.info("Found %s..." % os.path.basename(crawl_log))
                zipout.write(crawl_log, arcname=crawl_log[chop:])

            for log in glob.glob("%s/%s/%s/*-errors.log%s" % (
            LOCAL_LOG_ROOT, self.job.name, self.launch_id, get_stage_suffix(self.stage))):
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
    """
    Takes in the packaged logs and then parses the crawl log to look for WARC files, building up the whole package
    description for each crawl stage.
    """
    task_namespace = 'output'
    job = luigi.EnumParameter(enum=Jobs)
    launch_id = luigi.Parameter()
    stage = luigi.Parameter(default='final')

    def requires(self):
        if self.stage == 'final':
            yield CheckJobStopped(self.job, self.launch_id)
        yield PackageLogs(self.job, self.launch_id, self.stage)

    # TODO Move this into it's own job (atomicity):
    #    luigi.LocalTarget("%s/%s/%s/logs-%s.zip" % (self.LOCAL_LOG_ROOT, self.job.name, self.launch_id, self.stage))
    def output(self):
        return otarget(self.job, self.launch_id, self.stage)

    def run(self):

        logs = [self.get_crawl_log()]
        start_date = self.file_start_date(logs)
        # Find the WARCs referenced from the crawl log:
        (warcs, viral) = self.parse_crawl_log(logs)
        # TODO Look for WARCs not spotted via the logs and add them in (ALSO allow this in the log parser)
        if self.stage == 'final':
            for item in glob.glob(self.warc_file_path("*.warc.gz")):
                if item not in warcs:
                    logger.info("Found additional WARC: %s" % item)
                    warcs.append(item)
            #
            for item in glob.glob(self.viral_file_path("*.warc.gz")):
                if item not in warcs:
                    logger.info("Found additional Viral WARC: %s" % item)
                    warcs.append(item)

        # TODO Get sha512 and ARK identifiers for WARCs now, and store in launch folder and thus the zip?
        # Loop over all the WARCs involved
        i = 0
        hashes = {}
        for warc in warcs:
            # do some hard work here
            i += 1
            self.set_status_message = "Progress: Hashing WARC %d of %s" % (i, len(warcs))
            hash_output = yield CalculateLocalHash(self.job, self.launch_id, warc)
            # hash_file = yield HashLocalFile(warc)
            # with hash_file.open('r') as f
            with hash_output.open('r') as reader:
                sha = reader.read().rstrip('\n')
            hashes[warc] = sha
            # Report on progress...
            self.set_status_message = "Progress: Hashed WARC %d of %s" % (i, len(warcs))

        # Bundle logs and configuration data into a zip and upload it to HDFS
        zips = [ PackageLogs(self.job, self.launch_id, self.stage).output().path ]

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
            'hashes': hashes
        }

        with self.output().open('w') as f:
            f.write('{}'.format(json.dumps(job_output, indent=4)))

    def get_crawl_log(self):
        # First, parse the crawl log(s) and determine the WARC file names:
        logfilepath = "%s/%s/%s/crawl.log%s" % (
        LOCAL_LOG_ROOT, self.job.name, self.launch_id, get_stage_suffix(self.stage))
        logger.info("Looking for crawl logs stage: %s" % self.stage)
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
                if parts[1] == '-' or parts[1] == '' or int(parts[1]) <= 0:
                    if parts[1] == '':
                        logger.info("Skipping line with empty status! '%s' from log file '%s'" % (line, logs[0]))
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
                    logger.warning("No WARC file entry found for line: %s" % line)

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

        Also checks on HDFS if there is no local file.

        :type path: str
        """
        if os.path.exists(path) and os.path.isfile(path) and os.path.getsize(path) > 0:
            return True
        elif os.path.exists("%s.open" % path) and os.path.isfile("%s.open" % path) and os.path.getsize(
                        "%s.open" % path) > 0:
            return True
        elif get_hdfs_target(path).exists():
            return True
        else:
            return False


class AggregateOutputs(luigi.Task):
    """
    Takes the results of the ordered series of checkpoints/crawl stages and builds versioned aggregated packages
    from them.
    """
    task_namespace = 'output'
    job = luigi.EnumParameter(enum=Jobs)
    launch_id = luigi.Parameter()
    state = luigi.Parameter()
    outputs = luigi.ListParameter()

    def requires(self):
        for item in self.outputs:
            if item == 'crawl.log':
                stage = 'final'
            else:
                stage = item[-22:]
            yield AssembleOutput(self.job, self.launch_id, stage)

    def output(self):
        return atarget(self.job, self.launch_id, "%s.%s" %(len(self.outputs), self.state))

    def run(self):
        # Sort inputs by checkpoint timestamp and merge into versioned lists:
        aggregate = {}
        if isinstance(self.input(), list):
            inputs = self.input()
        else:
            inputs = [ self.input() ]
        # Build up the aggregate:
        for input in inputs:
            logger.info("Reading %s" % input.path)
            item = json.load(input.open())
            for key in item.keys():
                if isinstance(item[key],list):
                    current = aggregate.get(key, [])
                    current.extend(item[key])
                elif isinstance(item[key], dict):
                    current = aggregate.get(key, {})
                    current.update(item[key])
                elif item[key]:
                    current = item[key]
                aggregate[key] = current

        logger.info("Aggregate: %s" % aggregate)

        with self.output().open('w') as f:
            f.write('{}'.format(json.dumps(aggregate, indent=4)))


class ProcessOutputs(luigi.Task):
    """
    This looks at the checkpoints and final crawl chunk and sorts them by date, so an aggregate package for
    each new chunk can be composed. i.e. if there are two checkpoints and a final chunk, we should get three
    incremental packages containing (cp1), (cp1 cp2), (cp1 cp2 final).

    """
    task_namespace = 'output'
    job = luigi.EnumParameter(enum=Jobs)
    launch_id = luigi.Parameter()

    def requires(self):
        # FIXME Need to make sure this copes with crawl.log.TIMESTAMP etc. from failures.
        # Look for checkpoints to package, and package them in the correct order:
        outputs = {}
        is_final = False
        for item_path in glob.glob("%s/%s/%s/crawl.log*" % (LOG_ROOT, self.job.name, self.launch_id)):
            item = os.path.basename(item_path)
            logger.info("ITEM %s" % item)
            if item == "crawl.log":
                is_final = True
                outputs["final"] = item
            elif item.endswith(".lck"):
                pass
            else:
                outputs[item[-14:]] = item

        output_list = sorted(outputs.keys())
        logger.info("Ordered by date: %s" % output_list)

        aggregate = list()
        for key in output_list:
            aggregate.append(outputs[key])
            yield AggregateOutputs(self.job, self.launch_id, key, aggregate)

    def output(self):
        return atarget(self.job, self.launch_id, "list")

    def run(self):
        logger.info(self.launch_id)
        is_final = False
        outputs = []
        for input in self.input():
            if input.path.endswith(".final"):
                is_final = True
            outputs.append(input.path)

        # only report complete success if...
        if is_final:
            with self.output().open('w') as f:
                f.write('{}'.format(json.dumps(outputs, indent=4)))
        else:
            yield CheckJobStopped(self.job, self.launch_id)


class ScanForOutputs(luigi.WrapperTask):
    """
    This task scans the output folder for jobs and instances of those jobs, looking for crawled content to process.
    """
    task_namespace = 'output'
    date_interval = luigi.DateIntervalParameter(
        default=[datetime.date.today() - datetime.timedelta(days=1), datetime.date.today()])

    def requires(self):
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
                            yield ProcessOutputs(job, launch)


@AssembleOutput.event_handler(luigi.Event.SUCCESS)
def notify_success(task):
    """Will be called directly after a failed execution
       of `run` on any JobTask subclass
    """
    if slack().token and task.stage == 'final':
        sc = SlackClient(slack().token)
        print(sc.api_call(
            "chat.postMessage", channel="#crawls",
            text="Job %s finished and packaged successfully! :tada:"
                 % format_crawl_task(task), username='crawljobbot'))
        # , icon_emoji=':robot_face:'))
    else:
        logger.warning("No Slack auth token set, no message sent.")


if __name__ == '__main__':
    luigi.run(['output.ScanForOutputs', '--date-interval', '2016-11-01-2016-11-10'])  # , '--local-scheduler'])



