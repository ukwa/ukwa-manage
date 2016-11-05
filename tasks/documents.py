import re
from crawl.w3act.w3act import w3act
from common import *
from crawl_job_tasks import CheckJobStopped
import os
from urlparse import urlparse
import requests
from requests.utils import quote
import xml.dom.minidom

from crawl.dex.document_mdex import DocumentMDEx


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

    def complete(self):
        try:
            wburl = '%s/xmlquery.jsp?type=urlquery&url=%s' % (systems().wayback, quote(self.url))
            logger.debug("Checking %s" % wburl);
            r = requests.get(wburl)
            logger.debug("Response: %d" % r.status_code)
            # Is it known, with a matching timestamp?
            if r.status_code == 200:
                dom = xml.dom.minidom.parseString(r.text)
                for de in dom.getElementsByTagName('capturedate'):
                    if de.firstChild.nodeValue == self.ts:
                        # Excellent, it's been found:
                        return True
        except Exception as e:
            logger.error("%s [%s %s]" % (str(e), self.url, self.ts))
            logger.exception(e)
        # Otherwise:
        return False


class ScanLogForDocs(luigi.Task):
    """Watched the crawled documents log queue and passes entries to w3act

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
    job = luigi.EnumParameter(enum=Jobs)
    launch_id = luigi.Parameter()
    path = luigi.Parameter()
    stage = luigi.Parameter(default='final')

    def requires(self):
        if self.stage == 'final':
            yield CheckJobStopped(self.job, self.launch_id)

    def output(self):
        return dtarget(self.job, self.launch_id, self.stage)

    def run(self):
        """
        Parses the crawl log to look for documents.
        :return:
        """
        # First find the watched seeds list:
        with open("%s/%s/%s/watched-surts.txt" % (h3().local_job_folder, self.job.name, self.launch_id)) as reader:
            watched = [line.rstrip('\n') for line in reader]
            logger.info("WATCHED %s" % watched)
        # Then scan the logs for documents:
        line_count = 0
        with open(self.path, 'r') as f:
            for line in f:
                line_count += 1
                if line_count % 100 == 0:
                    self.set_status_message = "Currently at line %i of file %s" % (line_count, self.path)
                # And call method to extract documents from this line:
                yield self.process_document(line, watched)

    @staticmethod
    def process_document(line, watched):
        (timestamp, status_code, content_length, url, hop_path, via, mime,
         thread, start_time_plus_duration, hash, source, annotations) = re.split(" +", line, maxsplit=11)
        # Skip non-downloads:
        if status_code == '-' or status_code == '' or int(status_code) / 100 != 2:
            return
        # Check the URL and Content-Type:
        if "application/pdf" in mime:
            for prefix in watched:
                if url.startswith(prefix):
                    logger.info("Found document: %s" % line)
                    doc = {}
                    doc['wayback_timestamp'] = start_time_plus_duration[:14]
                    doc['landing_page_url'] = via
                    doc['document_url'] = url
                    doc['filename'] = os.path.basename(urlparse(url).path)
                    doc['size'] = int(content_length)

                    # Check if content appears to be in Wayback:
                    yield AvailableInWayback(doc['document_url'], doc['wayback_timestamp'])

                    # If so, lookup Target and extract any additional metadata:
                    w = w3act(act().url, act().username, act().password)
                    doc = DocumentMDEx(w, doc, source).mdex()
                    # Documents may be rejected at this point:
                    if doc is None:
                        logger.critical("The document based on this message has been REJECTED! :: %s" % line)
                        return
                    # Inform W3ACT it's available:
                    logger.debug("Sending doc: %s" % doc)
                    r = w.post_document(doc)
                    if r.status_code == 200:
                        logger.info("Document POSTed to W3ACT: %s" % doc['document_url'])
                        return
                    else:
                        logger.error("Failed with %s %s\n%s" % (r.status_code, r.reason, r.text))
                        raise Exception("Failed with %s %s\n%s" % (r.status_code, r.reason, r.text))


class ScanForDocuments(ScanForLaunches):
    """
    This task scans the output folder for jobs and instances of those jobs, looking for crawls logs.
    """
    task_namespace = 'doc'

    def scan_job_launch(self, job, launch):
        # Look for log files:
        outputs = {}
        is_final = False
        for item_path in glob.glob("%s/%s/%s/crawl.log*" % (LOG_ROOT, job.name, launch)):
            item = os.path.basename(item_path)
            if item == "crawl.log":
                is_final = True
                outputs["final"] = item_path
            elif item.endswith(".lck"):
                pass
            else:
                outputs[item[-14:]] = item_path

        output_list = sorted(outputs.keys())
        logger.info("Ordered by date: %s" % output_list)

        for key in output_list:
            yield ScanLogForDocs(job, launch, outputs[key], key)


if __name__ == '__main__':
    luigi.run(['doc.ScanForDocuments', '--date-interval', '2016-11-02-2016-11-10'])  # , '--local-scheduler'])
