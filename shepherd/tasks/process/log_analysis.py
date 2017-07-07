import os
import time
import json
import hashlib
import luigi.contrib.hdfs
import luigi.contrib.hadoop

from shepherd.tasks.w3act.feeds import CrawlFeed
from shepherd.tasks.process.scan_hdfs import ScanForOutputs
from shepherd.tasks.process.log_analysis_hadoop import AnalyseLogFile
from shepherd.tasks.process.documents import ExtractDocumentAndPost
from luigi.contrib.hdfs.format import Plain, PlainDir

from shepherd.tasks.common import logger
from shepherd.tasks.settings import state


class LogFilesForJobLaunch(luigi.ExternalTask):
    """
    On initialisation, looks up all logs current on HDFS for a particular job.

    Emits list of files to be processed.

    No run() as depends on external processes that produce the logs.
    """
    task_namespace = 'scan'
    job = luigi.Parameter()
    launch_id = luigi.Parameter()

    def output(self):
        outputs = []
        # Get HDFS client:
        client = luigi.contrib.hdfs.WebHdfsClient()
        parent_path = "/heritrix/output/logs/%s/%s" % (self.job, self.launch_id)
        for listed_item in client.listdir(parent_path):
            # Oddly, depending on the implementation, the listed_path may be absolute or basename-only, so fix here:
            item = os.path.basename(listed_item)
            item_path = os.path.join(parent_path, item)
            if item.endswith(".lck"):
                logger.error("Lock file should be be present on HDFS! %s" % (item, item_path))
                pass
            elif item.startswith("crawl.log"):
                outputs.append(luigi.contrib.hdfs.HdfsTarget(path=item_path, format=Plain))
                #logger.debug("Including %s" % item)
            else:
                pass
                #logger.debug("Skipping %s" % item)
        # Return the logs to be processed:
        return outputs


class SyncToHdfs(luigi.Task):
    """
    Designed to sync a single file up onto HDFS - intended for temp files while running jobs.
    """
    source_path = luigi.Parameter()
    target_path = luigi.Parameter()
    overwrite = luigi.BoolParameter(default=False)

    def complete(self):
        # Read local:
        local = luigi.LocalTarget(path=self.source_path)
        with local.open('r') as reader:
            local_hash = hashlib.sha512(reader.read()).hexdigest()
            logger.info("LOCAL HASH: %s" % local_hash)
        # Read from HDFS
        client = luigi.contrib.hdfs.WebHdfsClient()
        if not client.exists(self.target_path):
            return False
        with client.client.read(self.target_path) as reader:
            hdfs_hash = hashlib.sha512(reader.read()).hexdigest()
            logger.info("HDFS HASH: %s" % hdfs_hash)

        # If they match, we are good:
        return hdfs_hash == local_hash

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget(path=self.target_path, format=Plain)

    def run(self):
        client = luigi.contrib.hdfs.WebHdfsClient()
        # Upload to temp file:
        temp_path = "%s.temp" % self.target_path
        logger.info("Uploading to %s" % temp_path)
        with open(str(self.source_path)) as f:
            client.client.write(hdfs_path=temp_path, data=f.read(), overwrite=self.overwrite)
        # Remove any existing file, if we're allowed to:
        if self.overwrite:
            if client.exists(self.target_path):
                logger.info("Removing %s..." % self.target_path)
                client.remove(self.target_path, skip_trash=True)
        # And rename
        logger.info("Renaming to %s" % self.target_path)
        client.rename(temp_path, self.target_path)

        # Give the namenode a moment to catch-up with itself and then check it's there:
        # FIXME I suspect this is only needed for our ancient HDFS
        time.sleep(10)
        status = client.client.status(self.target_path)


class AnalyseAndProcessDocuments(luigi.Task):
    task_namespace = 'analyse'
    job = luigi.Parameter()
    launch_id = luigi.Parameter()
    log_paths = luigi.ListParameter()
    targets_path = luigi.Parameter()
    from_hdfs = luigi.BoolParameter(default=False)

    # Size of bunches of jobs to yield
    bunch_size = 1000

    def requires(self):
        return AnalyseLogFile(self.job, self.launch_id, self.log_paths, self.targets_path, self.from_hdfs)

    def output(self):
        return luigi.LocalTarget(
            '{}/documents/posted-{}-{}-{}.jsonl'.format(state().folder, self.job, self.launch_id, len(self.log_paths)))

    def run(self):
        # Loop over documents discovered, and attempt to post to W3ACT:
        with self.output().open('w') as out_file:
            with self.input().open() as in_file:
                counter = 0
                tasks = []
                for line in in_file:
                    prefix, docjson = line.strip().split("\t", 1)
                    if prefix.startswith("DOCUMENT"):
                        doc = json.loads(docjson)
                        out_file.write("%s\n" % json.dumps(doc))
                        tasks.append(ExtractDocumentAndPost(self.job, self.launch_id, doc, doc["source"]))
                        counter += 1
                        # Group tasks into bunches:
                        if counter%self.bunch_size == 0:
                            yield tasks
                            tasks = []


class GenerateCrawlLogReports(luigi.Task):
    """
    Via required tasks, launched M-R job to process crawl logs.

    Then runs through output documents and attempts to post them to W3ACT.
    """
    task_namespace = 'report'
    job = luigi.Parameter()
    launch_id = luigi.Parameter()
    extract_documents = luigi.BoolParameter(default=False)

    def requires(self):
        return LogFilesForJobLaunch(self.job, self.launch_id)

    def output(self):
        logs_count = len(self.input())
        if self.extract_documents:
            return luigi.LocalTarget(
                '{}/crawl-log-documents-{}-{}-{}'.format(state().folder, self.job, self.launch_id, logs_count))
        else:
            return luigi.LocalTarget(
                '{}/crawl-log-report-{}-{}-{}'.format(state().folder, self.job, self.launch_id, logs_count))

    def run(self):
        # Set up necessary data:
        feed = yield CrawlFeed(self.job)
        logs_count = len(self.input())

        # Cache targets in an appropriately unique filename (as unique as this task):
        hdfs_targets = yield SyncToHdfs(feed.path, '/tmp/cache/crawl-feed-%s-%s-%i.json' % (self.job, self.launch_id, logs_count), overwrite=True)

        # Turn the logs into a list:
        log_paths = []
        for log_file in self.input():
            log_paths.append(log_file.path)

        # Yield a task for processing all the current logs:
        log_stats = yield AnalyseLogFile(self.job, self.launch_id, log_paths, hdfs_targets.path, True)
        if self.extract_documents:
            yield AnalyseAndProcessDocuments(self.job, self.launch_id, log_paths, hdfs_targets.path, True)

        # And clean out the file from temp:
        logger.warning("Removing temporary targets cache: %s" % hdfs_targets.path)
        hdfs_targets.remove()


class ScanForLogs(ScanForOutputs):
    """
    This task scans the output folder for jobs and instances of those jobs, looking for crawls logs.
    """
    task_namespace = 'scan'
    scan_name = 'logs'

    def process_output(self, job, launch):
        yield GenerateCrawlLogReports(job, launch)


if __name__ == '__main__':
    luigi.run(['report.GenerateCrawlLogReports', '--job', 'weekly', '--launch-id', '20170220090024', '--local-scheduler'])
