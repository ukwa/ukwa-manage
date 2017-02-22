import re
import os
import json
import hashlib
import logging
from urlparse import urlparse
import requests
from requests.utils import quote
import xml.dom.minidom
import luigi.contrib.hdfs
import luigi.contrib.hadoop

from crawl.w3act.w3act import w3act
from crawl.h3.utils import url_to_surt
from crawl.dex.document_mdex import DocumentMDEx
from tasks.crawl.h3.crawl_job_tasks import CrawlFeed
from tasks.process.hadoop.crawl_summary import ScanForOutputs
from tasks.process.extract.documents_hadoop import ScanLogFileForDocs, LogFilesForJobLaunch
from tasks.common import target_name

logger = logging.getLogger('luigi-interface')

HDFS_PREFIX = os.environ.get("HDFS_PREFIX", "")
WAYBACK_PREFIX = os.environ.get("WAYBACK_PREFIX", "http://localhost:9080/wayback")

LUIGI_STATE_FOLDER = os.environ['LUIGI_STATE_FOLDER']
ACT_URL = os.environ['ACT_URL']
ACT_USER = os.environ['ACT_USER']
ACT_PASSWORD = os.environ['ACT_PASSWORD']


def dtarget(job, launch_id, status):
    return luigi.LocalTarget('{}/{}'.format(LUIGI_STATE_FOLDER, target_name('logs/documents', job, launch_id, status)))


class AvailableInWayback(luigi.ExternalTask):
    """

        Queries Wayback to see if the content is there yet.

        e.g.
        http://192.168.99.100:8080/wayback/xmlquery.jsp?type=urlquery&url=https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/497662/accidents-involving-illegal-alcohol-levels-2014.pdf

        <wayback>
        <request>
            <startdate>19960101000000</startdate>
            <resultstype>resultstypecapture</resultstype>
            <type>urlquery</type>
            <enddate>20160204115837</enddate>
            <firstreturned>0</firstreturned>
            <url>uk,gov)/government/uploads/system/uploads/attachment_data/file/497662/accidents-involving-illegal-alcohol-levels-2014.pdf
    </url>
            <resultsrequested>10000</resultsrequested>
            <resultstype>resultstypecapture</resultstype>
        </request>
        <results>
            <result>
                <compressedoffset>2563</compressedoffset>
                <mimetype>application/pdf</mimetype>
                <redirecturl>-</redirecturl>
                <file>BL-20160204113809800-00000-33~d39c9051c787~8443.warc.gz
    </file>
                <urlkey>uk,gov)/government/uploads/system/uploads/attachment_data/file/497662/accidents-involving-illegal-alcohol-levels-2014.pdf
    </urlkey>
                <digest>JK2AKXS4YFVNOTPS7Q6H2Q42WQ3PNXZK</digest>
                <httpresponsecode>200</httpresponsecode>
                <robotflags>-</robotflags>
                <url>https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/497662/accidents-involving-illegal-alcohol-levels-2014.pdf
    </url>
                <capturedate>20160204113813</capturedate>
            </result>
        </results>
    </wayback>
    """
    task_namespace = 'wb'
    url = luigi.Parameter()
    ts = luigi.Parameter()
    check_available = luigi.BoolParameter(default=False)

    resources = { 'qa-wayback': 1 }

    def complete(self):
        try:
            # Check if the item+timestamp is known:
            known = self.check_if_known()
            if self.check_available:
                if known:
                    # Check if the actual resource is available:
                    available = self.check_if_available()
                    return available
                # Otherwise:
                return False
            else:
                return known
        except Exception as e:
            logger.error("%s [%s %s]" % (str(e), self.url, self.ts))
            logger.exception(e)
        # Otherwise:
        return False

    def check_if_known(self):
        """
        Checks if a resource with a particular timestamp is available in the index:
        :return:
        """
        wburl = '%s/xmlquery.jsp?type=urlquery&url=%s' % (WAYBACK_PREFIX, quote(self.url))
        logger.debug("Checking availability %s" % wburl)
        r = requests.get(wburl)
        logger.debug("Availability response: %d" % r.status_code)
        # Is it known, with a matching timestamp?
        if r.status_code == 200:
            dom = xml.dom.minidom.parseString(r.text)
            for de in dom.getElementsByTagName('capturedate'):
                if de.firstChild.nodeValue == self.ts:
                    # Excellent, it's been found:
                    return True
        else:
            return False

    def check_if_available(self):
        """
        Checks if the resource is actually accessible/downloadable.

        This is done separately, as using this alone may accidentally get an older version.
        :return:
        """
        wburl = '%s/%s/%s' % (WAYBACK_PREFIX, self.ts, self.url)
        logger.debug("Checking download %s" % wburl)
        r = requests.head(wburl)
        logger.debug("Download HEAD response: %d" % r.status_code)
        # Resource is present?
        if r.status_code == 200:
            return True
        else:
            return False


class ExtractDocumentAndPost(luigi.Task):
    """
    Hook into w3act, extract MD and resolve the associated target.

    Note that the output file uses only the URL to make a hash-based identifier grouped by host, so this will only
    process each URL it sees once. This makes sense as the current model does not allow different
    Documents at the same URL in W3ACT.
    """
    task_namespace = 'doc'
    job = luigi.Parameter()
    launch_id = luigi.Parameter()
    doc = luigi.DictParameter()
    source = luigi.Parameter()

    resources = { 'w3act': 1 }

    def requires(self):
        return {
            'targets': CrawlFeed('frequent'),
            'available' : AvailableInWayback(self.doc['document_url'], self.doc['wayback_timestamp'])
        }

    @staticmethod
    def document_target(host, hash):
        return luigi.LocalTarget('{}/documents/{}/{}'.format(LUIGI_STATE_FOLDER, host, hash))

    def output(self):
        hasher = hashlib.md5()
        hasher.update(self.doc['document_url'])
        return self.document_target(urlparse(self.doc['document_url']).hostname, hasher.hexdigest())

    def run(self):
        # If so, lookup Target and extract any additional metadata:
        targets = json.load(self.input()['targets'].open('r'))
        doc = DocumentMDEx(targets, self.doc.get_wrapped().copy(), self.source).mdex()
        # Documents may be rejected at this point:
        if doc is None:
            logger.critical("The document %s has been REJECTED!" % self.doc['document_url'])
            doc = self.doc.get_wrapped().copy()
            doc['status'] = 'REJECTED'
        else:
            # Inform W3ACT it's available:
            doc['status'] = 'ACCEPTED'
            logger.debug("Sending doc: %s" % doc)
            w = w3act(ACT_URL, ACT_USER, ACT_PASSWORD)
            r = w.post_document(doc)
            if r.status_code == 200:
                logger.info("Document POSTed to W3ACT: %s" % doc['document_url'])
            else:
                logger.error("Failed with %s %s\n%s" % (r.status_code, r.reason, r.text))
                raise Exception("Failed with %s %s\n%s" % (r.status_code, r.reason, r.text))

        # And write out to the status file
        with self.output().open('w') as out_file:
            out_file.write('{}'.format(json.dumps(doc, indent=4)))


class ExtractDocuments(luigi.Task):
    """
    Via required tasks, launched M-R job to process crawl logs.

    Then runs through output documents and attempts to post them to W3ACT.
    """
    task_namespace = 'doc'
    job = luigi.Parameter()
    launch_id = luigi.Parameter()

    def requires(self):
        return LogFilesForJobLaunch(self.job, self.launch_id)

    def output(self):
        logs_count = len(self.input())
        return luigi.LocalTarget(
            '{}/documents/extracted-hadoop-{}-{}-{}'.format(LUIGI_STATE_FOLDER, self.job, self.launch_id, logs_count))

    def run(self):
        # Set up:
        feed = yield CrawlFeed(self.job)
        watched = self.get_watched_surts(feed)
        log_files = self.input()
        for log_file in log_files:
            docfile = yield ScanLogFileForDocs(self.job, self.launch_id, watched, log_file.path)
            # Loop over documents discovered, and attempt to post to W3ACT:
            with docfile.open() as in_file:
                for line in in_file:
                    url, docjson = line.strip().split("\t", 1)
                    doc = json.loads(docjson)
                    logger.error("Submission disabled! %s " % doc)
                    # yield ExtractDocumentAndPost(self.job, self.launch_id, doc, doc["source"])

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
        logger.info("WATCHED SURTS %s" % watched_surts)

        return watched_surts


class ScanForDocuments(ScanForOutputs):
    """
    This task scans the output folder for jobs and instances of those jobs, looking for crawls logs.
    """
    task_namespace = 'scan'
    scan_name = 'docs'

    def process_output(self, job, launch):
        yield ExtractDocuments(job, launch)


if __name__ == '__main__':
    luigi.run(['doc.ExtractDocuments', '--job', 'weekly', '--launch-id', '20170220090024', '--local-scheduler'])
    #luigi.run(['scan.ScanForDocuments', '--date-interval', '2017-02-10-2017-02-12', '--local-scheduler'])
