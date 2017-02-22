import re
import os
import json
import logging
from urlparse import urlparse
import requests
import dateutil, six
import luigi.contrib.hdfs
import luigi.contrib.hadoop
from luigi.contrib.hdfs.format import Plain

import crawl
from crawl.h3.utils import url_to_surt
from tasks.crawl.h3.crawl_job_tasks import CrawlFeed
from tasks.process.extract.documents import LUIGI_STATE_FOLDER, HDFS_PREFIX

logger = logging.getLogger('luigi-interface')


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
        client = luigi.contrib.hdfs.get_autoconfig_client()
        parent_path = "%s/heritrix/output/logs/%s/%s" % (HDFS_PREFIX, self.job, self.launch_id)
        for item_path in client.listdir(parent_path):
            item = os.path.basename(item_path)
            if item.endswith(".lck"):
                logger.error("Lock file should be be present on HDFS! %s" % (item, item_path))
                pass
            elif item.startswith("crawl.log"):
                outputs.append(luigi.contrib.hdfs.HdfsTarget(path=item_path, format=Plain))
                logger.debug("Including %s" % item)
            else:
                logger.debug("Skipping %s" % item)
        # Return the logs to be processed:
        return outputs


class ScanLogFileForDocsMR(luigi.contrib.hadoop.JobTask):
    """
    Map-Reduce job that scans a log file for documents associated with 'Watched' targets.

    Should run locally if run with only local inputs.

    Input:

    {
        "annotations": "ip:173.236.225.186,duplicate:digest",
        "content_digest": "sha1:44KA4PQA5TYRAXDIVJIAFD72RN55OQHJ",
        "content_length": 324,
        "extra_info": {},
        "hop_path": "IE",
        "host": "acid.matkelly.com",
        "jobName": "frequent",
        "mimetype": "text/html",
        "seed": "WTID:12321444",
        "size": 511,
        "start_time_plus_duration": "20160127211938966+230",
        "status_code": 404,
        "thread": 189,
        "timestamp": "2016-01-27T21:19:39.200Z",
        "url": "http://acid.matkelly.com/img.png",
        "via": "http://acid.matkelly.com/",
        "warc_filename": "BL-20160127211918391-00001-35~ce37d8d00c1f~8443.warc.gz",
        "warc_offset": 36748
    }

    Note that 'seed' is actually the source tag, and is set up to contain the original (Watched) Target ID.

    Output:

    [
    {
    "id_watched_target":<long>,
    "wayback_timestamp":<String>,
    "landing_page_url":<String>,
    "document_url":<String>,
    "filename":<String>,
    "size":<long>
    },
    <further documents>
    ]

    See https://github.com/ukwa/w3act/wiki/Document-REST-Endpoint

    i.e.

    seed -> id_watched_target
    start_time_plus_duration -> wayback_timestamp
    via -> landing_page_url
    url -> document_url (and filename)
    content_length -> size

    Note that, if necessary, this process to refer to the
    cdx-server and wayback to get more information about
    the crawled data and improve the landing page and filename data.


    """

    task_namespace = 'doc'
    job = luigi.Parameter()
    launch_id = luigi.Parameter()
    watched_surts = luigi.DictParameter()

    n_reduce_tasks = 1 # This is set to 1 as there is intended to be one output file.

    def requires(self):
        return LogFilesForJobLaunch(self.job, self.launch_id)

    def output(self):
        out_name = "%s.docs" % self.launch_id
        return luigi.contrib.hdfs.HdfsTarget(path=out_name, format=Plain)

    def extra_modules(self):
        return []

    def mapper(self, line):
        (timestamp, status_code, content_length, url, hop_path, via, mime,
         thread, start_time_plus_duration, hash, source, annotations) = re.split(" +", line, maxsplit=11)
        # Skip non-downloads:
        if status_code == '-' or status_code == '' or int(status_code) / 100 != 2:
            return
        # Check the URL and Content-Type:
        if "application/pdf" in mime:
            for prefix in self.watched_surts:
                document_surt = url_to_surt(url)
                landing_page_surt = url_to_surt(via)
                # Are both URIs under the same watched SURT:
                if document_surt.startswith(prefix) and landing_page_surt.startswith(prefix):
                    logger.info("Found document: %s" % line)
                    # Proceed to extract metadata and pass on to W3ACT:
                    doc = {
                        'wayback_timestamp': start_time_plus_duration[:14],
                        'landing_page_url': via,
                        'document_url': url,
                        'filename': os.path.basename(urlparse(url).path),
                        'size': int(content_length),
                        # Add some more metadata to the output so we can work out where this came from later:
                        'job_name': self.job,
                        'launch_id': self.launch_id,
                        'source': source
                    }
                    logger.info("Found document: %s" % doc)
                    yield url, json.dumps(doc)

    def reducer(self, key, values):
        """
        A pass-through reducer.

        :param key:
        :param values:
        :return:
        """
        for value in values:
            yield key, value


class ExtractDocuments(luigi.Task):
    """
    Via required tasks, launched M-R job to process crawl logs.

    Then runs through output documents and attempts to post them to W3ACT.
    """
    task_namespace = 'doc'
    job = luigi.Parameter()
    launch_id = luigi.Parameter()

    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget(
            '{}/documents/extracted-hadoop-{}-{}'.format(LUIGI_STATE_FOLDER, self.job, self.launch_id))

    def run(self):
        # Set up:
        feed = yield CrawlFeed(self.job)
        watched = self.get_watched_surts(feed)
        docfile = yield ScanLogFileForDocsMR(self.job, self.launch_id, watched)
        # Loop over documents discovered, and attempt to post to W3ACT:
        with docfile.open() as in_file:
            for line in in_file:
                url, docjson = line.strip().split("\t", 1)
                doc = json.loads(docjson)
                logger.error("Submission disabled! %s " % doc)
                #yield ExtractDocumentAndPost(self.job, self.launch_id, doc, doc["source"])

    def get_watched_surts(self, feed):
        # First find the unique watched seeds list:
        targets = json.load(feed.open())
        watched = set()
        for t in targets:
            if t['watched']:
                for seed in t['seeds']:
                    watched.add(seed)

        # Convert to SURT form:
        watched_surts = []
        for url in watched:
            watched_surts.append(url_to_surt(url))
        logger.info("WATCHED SURTS %s" % self.watched_surts)

        return watched_surts


if __name__ == '__main__':
    luigi.run(['doc.ExtractDocuments', '--job', 'weekly', '--launch-id', '20170220090024', '--local-scheduler'])
    #luigi.run(['scan.ScanForDocuments', '--date-interval', '2017-02-10-2017-02-12', '--local-scheduler'])
