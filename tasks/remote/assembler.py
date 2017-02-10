from __future__ import absolute_import

import os
import re
import json
import luigi
import luigi.contrib.ssh
import luigi.contrib.hdfs
import datetime
import gzip
import string
import hashlib
from dateutil.parser import parse
import zipfile
import logging
from tasks.heritrix.crawl_job_tasks import CheckJobStopped

logger = logging.getLogger('luigi-interface')

LUIGI_STATE_FOLDER = os.environ['LUIGI_STATE_FOLDER']
HDFS_PREFIX = os.environ['HDFS_PREFIX']
LOCAL_PREFIX = os.environ['LOCAL_PREFIX']
LOCAL_JOB_FOLDER = "%s%s" %( LOCAL_PREFIX, os.environ.get('LOCAL_JOB_FOLDER','/heritrix/jobs'))
LOCAL_OUTPUT_FOLDER = "%s%s" %( LOCAL_PREFIX, os.environ.get('LOCAL_OUTPUT_FOLDER','/heritrix/output') )
LOCAL_WREN_FOLDER = "%s%s" %( LOCAL_PREFIX, os.environ.get('LOCAL_WREN_FOLDER','/heritrix/wren') )
#SSH_KWARGS = { 'user': 'heritrix', 'key_file': 'id_rsa' }


def get_hdfs_target(path):
    # Prefix the original path with the HDFS root folder, stripping any leading '/' so the path is considered relative
    hdfs_path = os.path.join(HDFS_PREFIX, path.lstrip(os.path.sep))
    return luigi.contrib.hdfs.HdfsTarget(hdfs_path)


def target_name(state_class, job, launch_id, status):
    return '{}-{}/{}/{}/{}.{}.{}.{}'.format(launch_id[:4],launch_id[4:6], job.name, launch_id, state_class, job.name, launch_id, status)


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


class CalculateRemoteHash(luigi.Task):
    task_namespace = 'file'
    host = luigi.Parameter()
    path = luigi.Parameter()

    def output(self):
        return self.hash_target(self.job, self.launch_id, "%s.local.sha512" % self.path)

    def run(self):
        logger.debug("file %s to hash" % (self.path))

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
    def hash_target(job, launch_id, file):
        return luigi.LocalTarget('{}/{}'.format(LUIGI_STATE_FOLDER, target_name('files/hash', job, launch_id,
                                                                              os.path.basename(file))))


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
            return CheckJobStopped(self.job, self.launch_id, self.host)

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
            for crawl_log in rf.listdir("%s/logs/%s/%s/crawl.log%s" % (
                    LOCAL_OUTPUT_FOLDER, self.job.name, self.launch_id, get_stage_suffix(self.stage))):
                logger.info("Found %s..." % os.path.basename(crawl_log))
                zipout.write(crawl_log, arcname=crawl_log[chop:])

            for log in rf.listdir("%s/logs/%s/%s/*-errors.log%s" % (
                    LOCAL_OUTPUT_FOLDER, self.job.name, self.launch_id, get_stage_suffix(self.stage))):
                logger.info("Found %s..." % os.path.basename(log))
                zipout.write(log, arcname=log[chop:])

            for txt in rf.listdir("%s/%s/%s/*.txt" % (LOCAL_JOB_FOLDER, self.job.name, self.launch_id)):
                logger.info("Found %s..." % os.path.basename(txt))
                zipout.write(txt, arcname=txt[chop:])

            for txt in rf.listdir("%s/%s/%s/*.json" % (LOCAL_JOB_FOLDER, self.job.name, self.launch_id)):
                logger.info("Found %s..." % os.path.basename(txt))
                zipout.write(txt, arcname=txt[chop:])

            cxml = "%s/%s/%s/crawler-beans.cxml" % (LOCAL_JOB_FOLDER, self.job.name, self.launch_id)
            if rf.exists(cxml):
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
    host = luigi.Parameter()
    job = luigi.Parameter()
    launch_id = luigi.Parameter()
    stage = luigi.Parameter(default='final')

    def requires(self):
        if self.stage == 'final':
            yield CheckJobStopped(self.job, self.launch_id, self.host)
        yield PackageLogs(self.job, self.launch_id, self.stage)

    # TODO Move this into it's own job? (atomicity):
    #    luigi.LocalTarget("%s/%s/%s/logs-%s.zip" % (self.LOCAL_LOG_ROOT, self.job.name, self.launch_id, self.stage))
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
            for item in rf.listdir(self.warc_file_path("*.warc.gz")):
                if item not in warcs:
                    logger.info("Found additional WARC: %s" % item)
                    warcs.append(item)
            #
            for item in rf.listdir(self.viral_file_path("*.warc.gz")):
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
            hash_output = yield CalculateRemoteHash(self.job, self.launch_id, warc)
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

    def get_crawl_log(self, rf):
        # First, parse the crawl log(s) and determine the WARC file names:
        logfilepath = "%s/logs/%s/%s/crawl.log%s" % (LOCAL_OUTPUT_FOLDER, self.job.name,
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

    def warc_file_path(self, warcfile):
        return "%s/warcs/%s/%s/%s" % (LOCAL_OUTPUT_FOLDER, self.job.name, self.launch_id, warcfile)

    def viral_file_path(self, warcfile):
        return "%s/viral/%s/%s/%s" % (LOCAL_OUTPUT_FOLDER, self.job.name, self.launch_id, warcfile)

    def parse_crawl_log(self, logs):
        """
        Parses the crawl log to check the WARCs are present.
        :return:
        """
        # Set up remote connection:
        rf = luigi.contrib.ssh.RemoteFileSystem(self.host)
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
                    for wren in rf.listdir(
                                    "%s/%s*.warc.gz*" % (LOCAL_WREN_FOLDER, jmd['warcPrefix'])):
                        if wren.endswith('.open'):
                            wren = wren[:-5]
                        warcfiles.add(os.path.basename(wren))
                    # Also check in case file has already been moved into output/warcs/{job}/{launch}:
                    for wren in rf.listdir( self.warc_file_path("%s*.warc.gz*" % jmd['warcPrefix'])):
                        warcfiles.add(os.path.basename(wren))
                    # FIXME Also look on HDFS for matching files?
                else:
                    logger.warning("No WARC file entry found for line: %s" % line)

        warcs = []
        viral = []
        for warcfile in warcfiles:
            if self._file_exists(self.viral_file_path(warcfile), rf):
                logger.info("Found Viral WARC %s" % self.viral_file_path(warcfile))
                viral.append(self.viral_file_path(warcfile))
            elif self._file_exists("%s/%s" % (LOCAL_WREN_FOLDER, warcfile), rf):
                logger.info("Found WREN WARC %s" % warcfile)
                warcs.append("%s/%s" % (LOCAL_WREN_FOLDER, warcfile))
            elif self._file_exists(self.warc_file_path(warcfile), rf):
                logger.info("Found WARC %s" % self.warc_file_path(warcfile))
                warcs.append(self.warc_file_path(warcfile))
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
        if rf.exists(path) and rf.isfile(path):# and rf.getsize(path) > 0:
            return True
        elif rf.exists("%s.open" % path) and rf.isfile("%s.open" % path):# and rf.getsize("%s.open" % path) > 0:
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
            yield AssembleOutput(self.job, self.launch_id, stage)

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
    final_package_only = luigi.BoolParameter(default=True)

    def requires(self):
        # FIXME Need to make sure this copes with crawl.log.TIMESTAMP etc. from failures.
        # Set up remote connection:
        rf = luigi.contrib.ssh.RemoteFileSystem(self.host)
        # Look for checkpoints to package, and package them in the correct order:
        outputs = {}
        is_final = False
        for item_path in rf.listdir(self.host,
                                     "%s/logs/%s/%s/crawl.log*" % (LOCAL_OUTPUT_FOLDER, self.job.name, self.launch_id)):
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
            if not self.final_package_only:
                yield AggregateOutputs(self.job, self.launch_id, key, aggregate)
        # Only output the final package (not this intermediate ones):
        if self.final_package_only:
            yield AggregateOutputs(self.job, self.launch_id, "final", aggregate)

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
            yield CheckJobStopped(self.job, self.launch_id, self.host)


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
            for job_item in self.remote_ls("%s/*" % LOCAL_JOB_FOLDER, rf):
                job = os.path.basename(job_item)
                if rf.isdir(job_item):
                    launch_glob = "%s/%s*" % (job_item, date.strftime('%Y%m%d'))
                    logger.info("Looking for job launch folders matching %s" % launch_glob)
                    try:
                        for launch_item in self.remote_ls(launch_glob, rf):
                            logger.info("Found %s" % launch_item)
                            if rf.isdir(launch_item):
                                launch = os.path.basename(launch_item)
                                yield (self.host, job, launch)
                    except Exception as e:
                        # This pattern deals with non-existant directories by catching the exception.
                        logger.info("Error when listing.")
                        logger.exception(e)

    @staticmethod
    def remote_ls(path, rf):
        """
        Based on RemoteFileSystem.listdir but non-recursive
        :param path:
        :param remote_context:
        :return:
        """
        while path.endswith('/'):
            path = path[:-1]

        path = path or '.'

        listing = rf.remote_context.check_output(["ls", "-1d", path]).splitlines()
        return [v.decode('utf-8') for v in listing]




class ScanForOutputs(ScanForLaunches):
    """
    This task scans the output folder for jobs and instances of those jobs, looking for crawled content to process.
    """
    task_namespace = 'scan'
    scan_name = 'output'

    def scan_job_launch(self, host, job, launch):
        return ProcessOutputs(host, job, launch)


if __name__ == '__main__':
    luigi.run(['scan.ScanForOutputs', '--host', 'crawler07', '--date-interval', '2017-02-06-2017-02-08', '--local-scheduler'])
