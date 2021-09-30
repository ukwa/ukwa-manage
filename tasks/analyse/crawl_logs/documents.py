import os
import json
import hashlib
import logging
from urllib.parse import urlparse
import requests
from urllib.parse import quote_plus
import xml.dom.minidom
import luigi.contrib.hdfs
import luigi.contrib.hadoop

from w3act.client import w3act
from lib.docharvester.document_mdex import DocumentMDEx
from tasks.crawl.w3act import CrawlFeed, ENV_ACT_PASSWORD, ENV_ACT_URL, ENV_ACT_USER
from lib.targets import TaskTarget

logger = logging.getLogger(__name__)

# Define environment variable names here:
ENV_WAYBACK_URL_PREFIX = 'WAYBACK_URL_PREFIX'
ENV_CDXSERVER_ENDPOINT = 'CDXSERVER_ENDPOINT'

# Set up a common W3ACT connection to try to avoid constant re-logging-in
w3act_client = None
w3act_url = None

def get_w3act(wurl):
    global w3act_client, w3act_url
    if w3act_client is None or w3act_url != wurl:
        # Set the URL:
        w3act_url = wurl
        # Look up credentials and log into W3ACT:
        act_user = os.environ[ENV_ACT_USER]
        act_password = os.environ[ENV_ACT_PASSWORD]
        w3act_client = w3act(w3act_url, act_user, act_password)
    return w3act_client


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
    wayback_prefix = luigi.Parameter(default=os.environ[ENV_WAYBACK_URL_PREFIX])
    cdxserver_endpoint = luigi.Parameter(default=os.environ[ENV_CDXSERVER_ENDPOINT])

    resources = { 'qa-wayback': 1 }

    def complete(self):
        try:
            # Check if the item+timestamp is known:
            known = self.check_if_known()
            logger.info("")
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
        # See https://github.com/nla/outbackcdx/issues/12#issuecomment-351932136
        q = "type:urlquery url:" + quote_plus(self.url)
        wburl = "%s?q=%s" % (self.cdxserver_endpoint, quote_plus(q))
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

        # Otherwise, not found:
        return False

    def check_if_available(self):
        """
        Checks if the resource is actually accessible/downloadable.

        This is done separately, as using this alone may accidentally get an older version.
        :return:
        """
        wburl = '%s/%s/%s' % (self.wayback_prefix, self.ts, self.url)
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
    w3act = luigi.Parameter(default=os.environ[ENV_ACT_URL])

    resources = { 'w3act': 1 }

    def requires(self):
        return {
            'targets': CrawlFeed('all'),
            'available' : AvailableInWayback(self.doc['document_url'], self.doc['wayback_timestamp'])
        }

    @staticmethod
    def document_target(host, hash):
        return TaskTarget('documents','{}/{}'.format(host, hash))

    def output(self):
        hasher = hashlib.md5()
        hasher.update(self.doc['document_url'].encode('utf-8'))
        return self.document_target(urlparse(self.doc['document_url']).hostname, hasher.hexdigest())

    def run(self):
        # Set up a W3ACT client:
        w = get_w3act(self.w3act)

        # Lookup Target and extract any additional metadata:
        targets = json.load(self.input()['targets'].open('r'))
        doc = DocumentMDEx(targets, self.doc.get_wrapped().copy(), self.source).mdex()

        # Documents may be rejected at this point:
        if 'match_failed' in doc:
            logger.error("The document %s has been REJECTED!" % self.doc['document_url'])
            doc = self.doc.get_wrapped().copy()
            doc['status'] = 'REJECTED'
        else:
            # Inform W3ACT it's available:
            doc['status'] = 'ACCEPTED'
            logger.debug("Sending doc: %s" % doc)
            r = w.post_document(doc)
            if r.status_code == 200:
                logger.info("Document POSTed to W3ACT: %s" % doc['document_url'])
            else:
                logger.error("Failed with %s %s\n%s" % (r.status_code, r.reason, r.text))
                raise Exception("Failed with %s %s\n%s" % (r.status_code, r.reason, r.text))

        # And write out to the status file
        with self.output().open('w') as out_file:
            out_file.write('{}'.format(json.dumps(doc, indent=4)))
