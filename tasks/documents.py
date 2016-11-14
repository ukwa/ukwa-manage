import re
from crawl.w3act.w3act import w3act
from crawl.h3.utils import url_to_surt
from common import *
from move_to_hdfs import MoveFilesForLaunch
import os
import json
import hashlib
from urlparse import urlparse
import requests
from requests.utils import quote
import xml.dom.minidom
import luigi.contrib.esindex

from crawl.dex.document_mdex import DocumentMDEx
from tasks.crawl_job_tasks import CrawlFeed


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
        wburl = '%s/xmlquery.jsp?type=urlquery&url=%s' % (systems().wayback, quote(self.url))
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
        wburl = '%s/%s/%s' % (systems().wayback, self.ts, self.url)
        logger.debug("Checking download %s" % wburl)
        r = requests.head(wburl)
        logger.debug("Download HEAD response: %d" % r.status_code)
        # Resource is present?
        if r.status_code == 200:
            return True
        else:
            return False




class RecordDocumentInMonitrix(luigi.contrib.esindex.CopyToIndex):
    """
    Post the document to Monitrix, i.e. push into an appropriate Elasticsearch index.
    """
    task_namespace = 'doc'
    job = luigi.EnumParameter(enum=Jobs)
    launch_id = luigi.Parameter()
    doc = luigi.DictParameter()
    source = luigi.Parameter()

    host = systems().elasticsearch_host
    port = systems().elasticsearch_port
    index = "%s-documents-%s" % (systems().elasticsearch_index_prefix, datetime.datetime.now().strftime('%Y-%m-%d'))
    doc_type = 'default'
    purge_existing_index = False

    def requires(self):
        return ExtractDocumentAndPost(self.job, self.launch_id, self.doc, self.source)

    def docs(self):
        doc = json.load(self.input().open('r'))
        doc['timestamp'] = datetime.datetime.now().isoformat()
        return [ doc ]


class ExtractDocumentAndPost(luigi.Task):
    """
    Hook into w3act, extract MD and resolve the associated target.

    Note that the output file uses only the URL to make a hash-based identifier grouped by host, so this will only
    process each URL it sees once. This makes sense as the current model does not allow different
    Documents at the same URL in W3ACT.
    """
    task_namespace = 'doc'
    job = luigi.EnumParameter(enum=Jobs)
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
        return luigi.LocalTarget('{}/documents/{}/{}'.format(state().state_folder, host, hash))

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
            w = w3act(act().url, act().username, act().password)
            r = w.post_document(doc)
            if r.status_code == 200:
                logger.info("Document POSTed to W3ACT: %s" % doc['document_url'])
            else:
                logger.error("Failed with %s %s\n%s" % (r.status_code, r.reason, r.text))
                raise Exception("Failed with %s %s\n%s" % (r.status_code, r.reason, r.text))
                #yield AvailableInWayback(doc['document_url'], doc['wayback_timestamp'], check_available=True)

        # And write out to the status file
        with self.output().open('w') as out_file:
            out_file.write('{}'.format(json.dumps(doc, indent=4)))

        # Also post to Monitrix if configured to do so:
        if systems().elasticsearch_host:
            yield RecordDocumentInMonitrix(self.job, self.launch_id, doc, self.source)


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
        watched_surts = self.load_watched_surts()
        # Then scan the logs for documents:
        line_count = 0
        with open(self.path, 'r') as f:
            for line in f:
                line_count += 1
                if line_count % 100 == 0:
                    self.set_status_message = "Currently at line %i of file %s" % (line_count, self.path)
                # And yield tasks for each relevant document:
                (timestamp, status_code, content_length, url, hop_path, via, mime,
                 thread, start_time_plus_duration, hash, source, annotations) = re.split(" +", line, maxsplit=11)
                # Skip non-downloads:
                if status_code == '-' or status_code == '' or int(status_code) / 100 != 2:
                    continue
                # Check the URL and Content-Type:
                if "application/pdf" in mime:
                    for prefix in watched_surts:
                        surt = url_to_surt(url)
                        if surt.startswith(prefix):
                            logger.info("Found document: %s" % line)
                            # Proceed to extract metadata and pass on to W3ACT:
                            doc = {}
                            doc['wayback_timestamp'] = start_time_plus_duration[:14]
                            doc['landing_page_url'] = via
                            doc['document_url'] = url
                            doc['filename'] = os.path.basename(urlparse(url).path)
                            doc['size'] = int(content_length)
                            # Add some more metadata to the output so we can work out where this came from later:
                            doc['job_name'] = self.job.name
                            doc['launch_id'] = self.launch_id
                            doc['source'] = source
                            logger.info("Found document: %s" % doc)
                            yield ExtractDocumentAndPost(self.job, self.launch_id, doc, source)

    def output(self):
        return dtarget(self.job, self.launch_id, self.stage)

    def run(self):
        summary = []
        for it in self.input():
            summary.append(it.path)
        # And write out to the status file:
        with self.output().open('w') as out_file:
            out_file.write('{}'.format(json.dumps(summary, indent=4)))

    def load_watched_surts(self):
        # First find the watched seeds list:
        with open("%s/%s/%s/watched-surts.txt" % (h3().local_job_folder, self.job.name, self.launch_id)) as reader:
            watched = [line.rstrip('\n') for line in reader]
            logger.info("WATCHED %s" % watched)
        # Convert to SURT form:
        watched_surts = set()
        for url in watched:
            watched_surts.add(url_to_surt(url))
        logger.info("WATCHED SURTS %s" % watched_surts)
        return watched_surts


class ScanLogForDocsIfStopped(luigi.Task):
    task_namespace = 'doc'
    job = luigi.EnumParameter(enum=Jobs)
    launch_id = luigi.Parameter()
    path = luigi.Parameter()
    stage = luigi.Parameter(default='final')

    def requires(self):
        return MoveFilesForLaunch(self.job, self.launch_id)

    def output(self):
        return ScanLogForDocs(self.job, self.launch_id, self.path, self.stage).output()

    def run(self):
        yield ScanLogForDocs(self.job, self.launch_id, self.path, self.stage)


class ScanForDocuments(ScanForLaunches):
    """
    This task scans the output folder for jobs and instances of those jobs, looking for crawls logs.
    """
    task_namespace = 'scan'
    scan_name = 'docs'

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
            yield ScanLogForDocsIfStopped(job, launch, outputs[key], key)


# --- TESTS ---

class RunDocumentExtractionTests(luigi.Task):
    task_namespace = 'test'

    def requires(self):
        return {
            'targets': CrawlFeed('frequent')
        }

    def output(self):
        pass

    def run(self):
        # Non-matching Target test
        self.run_doc_mdex_test(
            'https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/567676/east_dulwich_community_nursery_association.pdf',
            'https://www.gov.uk/government/publications/east-dulwich-community-nursery-association-inquiry-report',
            'https://www.gov.uk/government/publications?departments[]=department-for-transport',
            None, "East Dulwich Community Nursery Association")

        # Title-only extraction tests:
        self.run_doc_mdex_test_extraction(
            "https://www.euromod.ac.uk/sites/default/files/working-papers/em2-01.pdf",
            "https://www.euromod.ac.uk/publications/date/2001/type/EUROMOD%20Working%20Paper%20Series",
            "https://www.euromod.ac.uk/", "Towards a multi purpose framework for tax benefit microsimulation")

        self.run_doc_mdex_test_extraction(
            "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/128968/competency-guidance.pdf",
            "https://www.gov.uk/government/organisations/department-for-work-pensions/about/recruitment",
            "https://www.gov.uk/government/organisations/department-for-work-pensions",
            "Guidance on writing competency statements for a job application")

        self.run_doc_mdex_test_extraction(
            "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/421402/List_of_lawyers_in_Mexico.pdf",
            "https://www.gov.uk/government/world/organisations/british-embassy-mexico-city",
            "https://www.gov.uk/government/publications?departments[]=department-for-transport",
            "List of lawyers and interpreters")

        # the tests Target association:
        # - scottish parliament
        self.run_doc_mdex_test('http://www.parliament.scot/S4_EducationandCultureCommittee/BBC charter/BBCcallforviews.pdf',
                          'http://www.parliament.scot/help/92650.aspx',
                          'http://www.parliament.scot/',
                          36096, "BBC charter renewal - Call for views")

        # - Children's Commissioner
        self.run_doc_mdex_test(
            'http://www.childrenscommissioner.gov.uk/sites/default/files/publications/The%20views%20of%20children%20and%20young%20people%20regarding%20media%20access%20to%20family%20courts.pdf',
            'http://www.childrenscommissioner.gov.uk/publications/report-views-children-and-young-people-regarding-media-access-family-courts',
            'http://www.childrenscommissioner.gov.uk/publications',
            36039, "Report on the views of children and young people regarding media access to family courts")

        # - ONS
        self.run_doc_mdex_test(
            'https://www.ons.gov.uk/peoplepopulationandcommunity/birthsdeathsandmarriages/ageing/articles/characteristicsofolderpeople/2013-12-06/pdf',
            'http://www.ons.gov.uk/peoplepopulationandcommunity/birthsdeathsandmarriages/ageing/articles/characteristicsofolderpeople/2013-12-06',
            '',
            36037,
            "Characteristics of Older People: What does the 2011 Census tell us about the \"oldest old\" living in England & Wales?")

        # - Notts CAMRA
        self.run_doc_mdex_test(
            'https://www.webarchive.org.uk/act-ddb/wayback/20160514170533/http://www.nottinghamcamra.org/festivals_720_2797277680.pdf',
            'http://www.nottinghamcamra.org/festivals.php',
            'http://nottinghamcamra.org',
            35989, "Beer Festivals")

        # - Local Government Association
        self.run_doc_mdex_test(
            'http://www.local.gov.uk/documents/10180/5716319/LGA+DECC+energy+efficiency+221113.pdf/86a87aaf-8650-4ef3-969b-3aff0e50083e',
            'http://www.local.gov.uk/web/guest/media-releases/-/journal_content/56/10180/5716193/NEWS',
            'http://www.local.gov.uk/publications',
            36040,
            "LGA press release 30 November 2013")  # page title: "Allow councils to lead energy efficiency schemes, says LGA")

        # - DCMS
        self.run_doc_mdex_test(
            'https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/522511/Research_to_explore_public_views_about_the_BBC_-_Wave_1_data_tables.pdf',
            'https://www.gov.uk/government/publications/research-to-explore-public-views-about-the-bbc',
            'https://www.gov.uk/government/publications?departments%5B%5D=department-for-culture-media-sport',
            36035, "Research to explore public views about the BBC - Data Tables Wave 1")

        # - ifs.org.uk
        self.run_doc_mdex_test('http://www.ifs.org.uk/uploads/cemmap/wps/cwp721515.pdf',
                          'http://www.ifs.org.uk/publications/8080', 'http://www.ifs.org.uk',
                          35915, "Identifying effects of multivalued treatments")
        self.run_doc_mdex_test('http://www.ifs.org.uk/uploads/publications/bns/BN179.pdf',
                          'http://www.ifs.org.uk/publications/8049', 'http://www.ifs.org.uk',
                          35915, "Funding the English & Welsh police service: from boom to bust?")

        # - gov.uk
        self.run_doc_mdex_test(
            'https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/507081/2904936_Bean_Review_Web_Accessible.pdf',
            'https://www.gov.uk/government/publications/independent-review-of-uk-economic-statistics-final-report',
            'https://www.gov.uk/publications',
            35909, "Independent review of UK economic statistics: final report")
        self.run_doc_mdex_test('https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/246770/0121.pdf',
                          'https://www.gov.uk/government/publications/met-office-annual-report-and-accounts-2012-to-2013',
                          'https://www.gov.uk/',
                          35913, "Met Office annual report and accounts 2012/13 - Full Text")
        self.run_doc_mdex_test(
            'https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/497536/rtfo-year-8-report-2.pdf',
            'https://www.gov.uk/government/statistics/biofuel-statistics-year-8-2015-to-2016-report-2',
            'https://www.gov.uk/',
            35846, "Renewable Transport Fuel Obligation statistics: year 8, report 2")
        self.run_doc_mdex_test(
            'https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/495227/harbour-closure-orders-consultation-summary-responses.pdf',
            'https://www.gov.uk/government/consultations/harbour-closure-and-pilotage-function-removal-orders-draft-guidance',
            'https://www.gov.uk/',
            35846, "Guidance on harbour closure orders and pilotage function removal orders: summary of responses")

    def run_doc_mdex_test(self, url, lpu, src, tid, title):
        logger.info("Looking at document URL: %s" % url)
        doc = {}
        doc['document_url'] = url
        doc['landing_page_url'] = lpu
        targets = json.load(self.input()['targets'].open('r'))
        doc = DocumentMDEx(targets, doc, src, null_if_no_target_found=False).mdex()
        logger.info(json.dumps(doc))
        if doc['target_id'] != tid:
            raise Exception("Target matching failed! %s v %s" % (doc['target_id'], tid))
        if doc.get('title', None) != title:
            raise Exception("Wrong title found for this document! '%s' v '%s'" % (doc['title'], title))

    def run_doc_mdex_test_extraction(self, url, lpu, src, title):

        logger.info("Looking at document URL: %s" % url)
        doc = {}
        doc['document_url'] = url
        doc['landing_page_url'] = lpu
        targets = json.load(self.input()['targets'].open('r'))
        doc = DocumentMDEx(targets, doc, src, null_if_no_target_found=False).mdex()
        logger.info(json.dumps(doc))
        if doc.get('title', None) != title:
            raise Exception("Wrong title found for this document! '%s' v '%s'" % (doc['title'], title))


if __name__ == '__main__':
    #luigi.run(['scan.ScanForDocuments', '--date-interval', '2016-11-04-2016-11-10'])  # , '--local-scheduler'])
    luigi.run(['test.RunDocumentExtractionTests'])
