from __future__ import absolute_import

import os
import re
import json
import luigi
import luigi.contrib.ssh
import luigi.contrib.hdfs
import luigi.contrib.hdfs.error
import datetime
import gzip
import string
import hashlib
from dateutil.parser import parse
import zipfile
import logging
import tempfile

logger = logging.getLogger('luigi-interface')

LUIGI_STATE_FOLDER = os.environ['LUIGI_STATE_FOLDER']
HDFS_PREFIX = os.environ['HDFS_PREFIX']
LOCAL_PREFIX = os.environ['LOCAL_PREFIX']
LOCAL_JOB_FOLDER = "%s%s" %( LOCAL_PREFIX, os.environ.get('LOCAL_JOB_FOLDER','/heritrix/jobs'))
LOCAL_OUTPUT_FOLDER = "%s%s" %( LOCAL_PREFIX, os.environ.get('LOCAL_OUTPUT_FOLDER','/heritrix/output') )
LOCAL_WREN_FOLDER = "%s%s" %( LOCAL_PREFIX, os.environ.get('LOCAL_WREN_FOLDER','/heritrix/wren') )


def get_hdfs_target(path):
    # Chop out any local prefix:
    hdfs_path = path[len(LOCAL_PREFIX):]
    # Prefix the original path with the HDFS root folder, stripping any leading '/' so the path is considered relative
    hdfs_path = os.path.join(HDFS_PREFIX, hdfs_path.lstrip("/"))
    return luigi.contrib.hdfs.HdfsTarget(hdfs_path)


def target_name(state_class, job, launch_id, status):
    return '{}-{}/{}/{}/{}.{}.{}.{}'.format(launch_id[:4],launch_id[4:6], job, launch_id, state_class, job, launch_id, status)


def jtarget(job, launch_id, status):
    return luigi.LocalTarget('{}/{}'.format(LUIGI_STATE_FOLDER, target_name('01.jobs', job, launch_id, status)))

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


def remote_ls(parent, glob, rf):
    """
    Based on RemoteFileSystem.listdir but non-recursive.

    :param parent:
    :param glob:
    :param rf:
    :return:
    """
    while parent.endswith('/'):
        parent = parent[:-1]

    parent = parent or '.'

    # If the parent folder does not exists, there are no matches:
    if not rf.exists(parent):
        return []

    listing = rf.remote_context.check_output(["find", "-L", parent, '-maxdepth', '1', '-name', '"%s"' % glob]).splitlines()
    return [v.decode('utf-8') for v in listing]

class StopJobExternalTask(luigi.ExternalTask):
    """
    This task is used to mark jobs as stopped, but this is not something that can be forced automatically, see StopJob.
    """
    task_namespace = 'output'
    host = luigi.Parameter()
    job = luigi.Parameter()
    launch_id = luigi.Parameter()

    def output(self):
        return jtarget(self.job, self.launch_id, 'stopped')


class CheckJobStopped(luigi.Task):
    """
    Checks if given job/launch is currently running. Will not force the crawl to stop.
    """
    task_namespace = 'output'
    host = luigi.Parameter()
    job = luigi.Parameter()
    launch_id = luigi.Parameter()

    def output(self):
        return jtarget(self.job, self.launch_id, 'stopped')

    def run(self):
        # Set up connection to H3:
        rf = luigi.contrib.ssh.RemoteFileSystem(self.host)

        # Is that job running?
        if rf.exists("%s/logs/%s/%s/crawl.log.lck" % (LOCAL_OUTPUT_FOLDER, self.job, self.launch_id)) or \
                rf.exists("%s/logs/%s/%s/job.log.lck" % (LOCAL_OUTPUT_FOLDER, self.job, self.launch_id)):
            # Declare that we are awaiting an external process to stop this job:
            yield StopJobExternalTask(self.host, self.job, self.launch_id)

        # Not running, so mark as stopped:
        with self.output().open('w') as f:
            f.write('{} {}\n'.format(self.job, self.launch_id))


class CalculateRemoteHash(luigi.Task):
    task_namespace = 'output'
    host = luigi.Parameter()
    path = luigi.Parameter()

    def output(self):
        return self.hash_target("%s.local.sha512" % self.path)

    def run(self):
        logger.debug("file %s to hash" % self.path)

        t = luigi.contrib.ssh.RemoteTarget(self.path,self.host)#, **SSH_KWARGS)
        with t.open('r') as reader:
            file_hash = hashlib.sha512(reader.read()).hexdigest()

        # test hash
        CalculateRemoteHash.check_hash(self.path, file_hash)

        with self.output().open('w') as f:
            f.write(file_hash)

    @staticmethod
    def check_hash(path, file_hash):
        logger.debug("Checking file %s hash %s" % (path, file_hash))
        if len(file_hash) != 128:
            raise Exception("%s hash not 128 character length [%s]" % (path, len(file_hash)))
        if not all(c in string.hexdigits for c in file_hash):
            raise Exception("%s hash not all hex [%s]" % (path, file_hash))

    @staticmethod
    def hash_target(file):
        return luigi.LocalTarget('{}/{}/{}'.format(LUIGI_STATE_FOLDER,
                                                   'files/hash', os.path.basename(file)))


class PackageLogs(luigi.Task):
    """
    For a given job launch and stage, package up the logs and config in a ZIP to go inside the main crawl SIP.
    """
    task_namespace = 'output'
    host = luigi.Parameter()
    job = luigi.Parameter()
    launch_id = luigi.Parameter()
    stage = luigi.Parameter()

    def requires(self):
        if self.stage == 'final':
            return CheckJobStopped(self.host, self.job, self.launch_id)

    def output(self):
        return luigi.LocalTarget('{}/{}.zip'.format(LUIGI_STATE_FOLDER,
                                                    target_name('02.logs', self.job, self.launch_id, self.stage)))

    def run(self):
        """Zips up all log/config. files and copies said archive to HDFS; finds the
        earliest timestamp in the logs."""
        # Set up remote connection:
        rf = luigi.contrib.ssh.RemoteFileSystem(self.host)
        # Set up the output, first making sure the full path exists:
        with self.output().open('w') as f:
            f.write('')
        self.output().remove()
        # What to remove from the paths:
        chop = len(str(LOCAL_PREFIX))
        with zipfile.ZipFile(self.output().path, 'w', allowZip64=True) as zipout:
            # Crawl log:
            for crawl_log in remote_ls(
                            "%s/logs/%s/%s" % (LOCAL_OUTPUT_FOLDER, self.job, self.launch_id),
                            "/crawl.log%s" % get_stage_suffix(self.stage), rf):
                logger.info("Found %s..." % os.path.basename(crawl_log))
                self.add_remote_file(zipout, crawl_log[chop:], crawl_log, rf)
            # Error log(s)
            for log in remote_ls(
                            "%s/logs/%s/%s" % (LOCAL_OUTPUT_FOLDER, self.job, self.launch_id),
                            "/*-errors.log%s" % get_stage_suffix(self.stage), rf):
                logger.info("Found %s..." % os.path.basename(log))
                self.add_remote_file(zipout, log[chop:], log, rf)

            # Job text files
            for txt in remote_ls(
                            "%s/%s/%s" % (LOCAL_JOB_FOLDER, self.job, self.launch_id),
                            "/*.txt", rf):
                self.add_remote_file(zipout, txt[chop:], txt, rf)

            # Job json files:
            for txt in remote_ls(
                            "%s/%s/%s" % (LOCAL_JOB_FOLDER, self.job, self.launch_id),
                            "/*.json", rf):
                logger.info("Found %s..." % os.path.basename(txt))
                self.add_remote_file(zipout, txt[chop:], txt, rf)
            # Job crawler definition:
            cxml = "%s/%s/%s/crawler-beans.cxml" % (LOCAL_JOB_FOLDER, self.job, self.launch_id)
            if rf.exists(cxml):
                logger.info("Found config...")
                self.add_remote_file(zipout, cxml[chop:], cxml, rf)
            else:
                logger.error("Cannot find config.")
                raise Exception("Cannot find config.")

    def add_remote_file(self, zipout, zippath, remotepath, rf):
        local_temp = tempfile.NamedTemporaryFile(delete=False).name
        rf.get(remotepath, local_temp)
        zipout.write(local_temp, arcname=zippath)
        os.remove(local_temp)


class AssembleOutput(luigi.Task):
    """
    Takes in the packaged logs and then parses the crawl log to look for WARC files, building up the whole package
    description for each crawl stage.
    """
    task_namespace = 'output'
    host = luigi.Parameter()
    job = luigi.Parameter()
    launch_id = luigi.Parameter()
    stage = luigi.Parameter(default='final')

    def requires(self):
        if self.stage == 'final':
            yield CheckJobStopped(self.host, self.job, self.launch_id)
        yield PackageLogs(self.host, self.job, self.launch_id, self.stage)

    # TODO Move this into it's own job? (atomicity):
    #    luigi.LocalTarget("%s/%s/%s/logs-%s.zip" % (self.LOCAL_LOG_ROOT, self.job, self.launch_id, self.stage))
    def output(self):
        return luigi.LocalTarget(
            '{}/{}'.format(LUIGI_STATE_FOLDER, target_name('03.outputs', self.job, self.launch_id, self.stage)))

    def run(self):
        # Set up remote connection:
        rf = luigi.contrib.ssh.RemoteFileSystem(self.host)
        logs = [self.get_crawl_log(rf)]
        start_date = self.file_start_date(logs)
        # Find the WARCs referenced from the crawl log:
        (warcs, viral) = self.parse_crawl_log(logs)
        # TODO Look for WARCs not spotted via the logs and add them in (ALSO allow this in the log parser)
        if self.stage == 'final':
            for item in remote_ls(self.warc_file_path(), "*.warc.gz", rf):
                if item not in warcs:
                    logger.info("Found additional WARC: %s" % item)
                    warcs.append(item)
            #
            for item in remote_ls(self.viral_file_path(), "*.warc.gz", rf):
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
            hash_output = yield CalculateRemoteHash(self.host, warc)
            with hash_output.open('r') as reader:
                sha = reader.read().rstrip('\n')
            hashes[warc] = sha
            # Report on progress...
            self.set_status_message = "Progress: Hashed WARC %d of %s" % (i, len(warcs))

        # Bundle logs and configuration data into a zip and upload it to HDFS
        zips = [ PackageLogs(self.host, self.job, self.launch_id, self.stage).output().path ]

        # FIXME Need to mint and add in ARKs at this point:

        # Output the job package summary:
        job_output = {
            'job_id': self.job,
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

    def get_crawl_log(self, rf):
        # First, parse the crawl log(s) and determine the WARC file names:
        logfilepath = "%s/logs/%s/%s/crawl.log%s" % (LOCAL_OUTPUT_FOLDER, self.job,
                                                     self.launch_id, get_stage_suffix(self.stage))
        logger.info("Looking for crawl logs stage: %s" % self.stage)
        logger.info("Looking for crawl logs: %s" % logfilepath)
        if rf.exists(logfilepath):
            logger.info("Found %s..." % os.path.basename(logfilepath))
            return logfilepath
        else:
            raise Exception("Log file '%s' not found!" % logfilepath)

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

    def warc_file_path(self):
        return "%s/warcs/%s/%s" % (LOCAL_OUTPUT_FOLDER, self.job, self.launch_id)

    def viral_file_path(self):
        return "%s/viral/%s/%s" % (LOCAL_OUTPUT_FOLDER, self.job, self.launch_id)

    def parse_crawl_log(self, logs):
        """
        Parses the crawl log to check the WARCs are present.
        :return:
        """
        # Set up remote connection:
        rf = luigi.contrib.ssh.RemoteFileSystem(self.host)
        warcfiles = set()
        remote_log = luigi.contrib.ssh.RemoteTarget(logs[0], self.host)
        with remote_log.open('r') as f:
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
                    for wren in remote_ls( LOCAL_WREN_FOLDER, "%s*.warc.gz*" % jmd['warcPrefix'], rf):
                        if wren.endswith('.open'):
                            wren = wren[:-5]
                        warcfiles.add(os.path.basename(wren))
                    # Also check in case file has already been moved into output/warcs/{job}/{launch}:
                    for wren in remote_ls(self.warc_file_path(), "%s*.warc.gz*" % jmd['warcPrefix'], rf):
                        warcfiles.add(os.path.basename(wren))
                    # FIXME Also look on HDFS for matching files?
                else:
                    logger.warning("No WARC file entry found for line: %s" % line)

        warcs = []
        viral = []
        for warcfile in warcfiles:
            if self._file_exists("%s/%s" % (self.viral_file_path(), warcfile), rf):
                logger.info("Found Viral WARC %s/%s" % (self.viral_file_path(), warcfile))
                viral.append("%s/%s" % (self.viral_file_path(), warcfile))
            elif self._file_exists("%s/%s" % (LOCAL_WREN_FOLDER, warcfile), rf):
                logger.info("Found WREN WARC %s" % warcfile)
                warcs.append("%s/%s" % (LOCAL_WREN_FOLDER, warcfile))
            elif self._file_exists("%s/%s" % (self.warc_file_path(), warcfile), rf):
                logger.info("Found WARC %s/%s" % (self.warc_file_path(),warcfile))
                warcs.append("%s/%s" % (self.warc_file_path(), warcfile))
            else:
                raise Exception("Cannot file warc file %s" % warcfile)

        return warcs, viral

    @staticmethod
    def _file_exists(path, rf):
        """
        Checks whether the given file exists and has content - allowed to be '.open' at this point.

        Also checks on HDFS if there is no local file.

        :type path: str
        """
        logger.info("Looking for %s" % path)
        if rf.exists(path) and not rf.isdir(path):# and rf.getsize(path) > 0:
            return True
        elif rf.exists("%s.open" % path) and not rf.isdir("%s.open" % path):# and rf.getsize("%s.open" % path) > 0:
            return True
        else:
            try:
                if get_hdfs_target(path).exists():
                    return True
            except luigi.contrib.hdfs.error.HDFSCliError as e:
                logger.error("Exception while checking HDFS.")
                logger.exception(e)
            return False


class AggregateOutputs(luigi.Task):
    """
    Takes the results of the ordered series of checkpoints/crawl stages and builds versioned aggregated packages
    from them.
    """
    task_namespace = 'output'
    host = luigi.Parameter()
    job = luigi.Parameter()
    launch_id = luigi.Parameter()
    state = luigi.Parameter()
    outputs = luigi.ListParameter()

    def requires(self):
        for item in self.outputs:
            if item == 'crawl.log':
                stage = 'final'
            else:
                stage = item[-22:]
            yield AssembleOutput(self.host, self.job, self.launch_id, stage)

    def output(self):
        return luigi.LocalTarget(
                '{}/{}'.format(LUIGI_STATE_FOLDER, target_name('04.assembled', self.job, self.launch_id,
                                                               "%s.%s" %(len(self.outputs), self.state))))

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
    host = luigi.Parameter()
    job = luigi.Parameter()
    launch_id = luigi.Parameter()

    def requires(self):
        # FIXME Need to make sure this copes with crawl.log.TIMESTAMP etc. from failures.
        # Set up remote connection:
        rf = luigi.contrib.ssh.RemoteFileSystem(self.host)
        # Look for checkpoints to package, and package them in the correct order:
        outputs = {}
        is_final = False
        for item_path in remote_ls("%s/logs/%s/%s" % (LOCAL_OUTPUT_FOLDER, self.job, self.launch_id), "crawl.log*", rf):
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

        # Build up the list of stages, in order, and aggregate:
        aggregate = list()
        for key in output_list:
            aggregate.append(outputs[key])
            yield AggregateOutputs(self.host, self.job, self.launch_id, key, aggregate)

    def output(self):
        return luigi.LocalTarget(
                '{}/{}'.format(LUIGI_STATE_FOLDER, target_name('04.assembled', self.job, self.launch_id, "list")))

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
            yield CheckJobStopped(self.host, self.job, self.launch_id)


class ScanForLaunches(luigi.WrapperTask):
    """
    This task scans the output folder for jobs and instances of those jobs, looking for crawled content to process.

    Sub-class this and override the scan_job_launch method as needed.
    """
    task_namespace = 'scan'
    host = luigi.Parameter()
    date_interval = luigi.DateIntervalParameter(
        default=[datetime.date.today() - datetime.timedelta(days=1), datetime.date.today()])
    timestamp = luigi.DateMinuteParameter(default=datetime.datetime.today())

    def requires(self):
        # Enumerate the jobs:
        for (host, job, launch) in self.enumerate_launches():
            logger.info("Processing %s:%s/%s" % ( host, job, launch ))
            yield self.scan_job_launch(host, job, launch)

    def enumerate_launches(self):
        # Set up connection:
        rf = luigi.contrib.ssh.RemoteFileSystem(self.host)
        # Look for jobs that need to be processed:
        for date in self.date_interval:
            for job_item in remote_ls(LOCAL_JOB_FOLDER, "*", rf):
                job = os.path.basename(job_item)
                if rf.isdir(job_item):
                    launch_glob = "%s*" % date.strftime('%Y%m%d')
                    logger.info("Looking for job launch folders matching %s" % launch_glob)
                    for launch_item in remote_ls(job_item, launch_glob, rf):
                        logger.info("Found %s" % launch_item)
                        if rf.isdir(launch_item):
                            launch = os.path.basename(launch_item)
                            yield (self.host, job, launch)


class ScanForOutputs(ScanForLaunches):
    """
    This task scans the output folder for jobs and instances of those jobs, looking for crawled content to process.
    """
    task_namespace = 'scan'
    scan_name = 'output'

    def scan_job_launch(self, host, job, launch):
        return ProcessOutputs(host, job, launch)


if __name__ == '__main__':
    luigi.run(['scan.ScanForOutputs', '--host', 'localhost', '--date-interval', '2017-01-01-2017-01-03', '--local-scheduler'])
