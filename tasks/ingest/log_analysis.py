import os
import time
import json
import hashlib
import luigi.contrib.hdfs
import luigi.contrib.hadoop
from luigi.contrib.hdfs.format import Plain, PlainDir

from tasks.ingest.log_analysis_hadoop import AnalyseLogFile, SummariseLogFiles
from tasks.ingest.documents import ExtractDocumentAndPost
from tasks.ingest.w3act import CrawlFeed
from tasks.common import state_file, logger
from lib.webhdfs import webhdfs
from lib.targets import TaskTarget


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
        return TaskTarget('documents', 'posted-{}-{}-{}.jsonl'.format(self.job, self.launch_id, len(self.log_paths)))

    def run(self):
        # Loop over documents discovered, and attempt to post to W3ACT:
        with self.output().open('w') as out_file:
            with self.input().open() as in_file:
                counter = 0
                tasks = []
                for line in in_file:
                    logger.info("Got line: %s" % line)
                    prefix, docjson = line.strip().split("\t", 1)
                    if prefix.startswith("DOCUMENT"):
                        doc = json.loads(docjson)
                        logger.info("Got doc: %s" % doc['document_url'])
                        out_file.write("%s\n" % json.dumps(doc))
                        edp_task = ExtractDocumentAndPost(self.job, self.launch_id, doc, doc["source"])
                        # Reduce Luigi scheduler overhead by only enqueuing incomplete tasks:
                        if not edp_task.complete():
                            tasks.append(edp_task)
                            counter += 1
                        # Group tasks into bunches:
                        if counter%self.bunch_size == 0:
                            yield tasks
                            tasks = []
                if len(tasks) > 0:
                    yield tasks


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
            return TaskTarget('crawl-reports',
                'crawl-log-documents-{}-{}-{}'.format(self.job, self.launch_id, logs_count))
        else:
            return TaskTarget('crawl-reports',
                'crawl-log-report-{}-{}-{}'.format(self.job, self.launch_id, logs_count))

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

        # Yield a task for processing all the current logs (Hadoop job):
        log_stats = yield AnalyseLogFile(self.job, self.launch_id, log_paths, hdfs_targets.path, True)

        # If we are looking at documents, extract them:
        if self.extract_documents:
            yield AnalyseAndProcessDocuments(self.job, self.launch_id, log_paths, hdfs_targets.path, True)

        # And clean out the file from temp:
        logger.warning("Removing temporary targets cache: %s" % hdfs_targets.path)
        hdfs_targets.remove()


class DomainCrawlSummarise(luigi.WrapperTask):
    dc_id = luigi.Parameter(default='20170515')

    task_namespace = 'analyse'

    def requires(self):
        h = webhdfs()
        logs = []
        for path in ["/heritrix/output/logs/dc0-%s" % self.dc_id, "/heritrix/output/logs/dc1-%s" % self.dc_id,
                     "/heritrix/output/logs/dc2-%s" % self.dc_id, "/heritrix/output/logs/dc3-%s" % self.dc_id]:
            for item in h.list(path):
                if item.startswith('crawl.log'):
                    logs.append("%s/%s" % (path,item))
        print("Found %i log files..." % len(logs))
        logger.info("Found %i log files..." % len(logs))
        yield SummariseLogFiles(logs,'dc',self.dc_id,True)


if __name__ == '__main__':
    import logging
    logging.getLogger().setLevel(logging.INFO)
    #luigi.run(['analyse.DomainCrawlSummarise', '--local-scheduler'])
    luigi.run(['report.GenerateCrawlLogReports',
               '--job', 'weekly',
               '--launch-id', '20180507080102',
               '--extract-documents'])
