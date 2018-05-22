import json
import luigi
import logging
from lib.docharvester.document_mdex import DocumentMDEx

logger = logging.getLogger('luigi-interface')


class RunDocumentExtractionTests(luigi.Task):
    """
    This test task runs some metadata extraction tests.
    """
    task_namespace = 'test'

    @staticmethod
    def load_targets():
        with open('../../test/crawl-feed.2018-05-21T2100.weekly.json') as f:
            return json.load(f)

    def run(self):
        # FIXME Add tests for Command and Act papers, ISBN,

        # Examples of the new website layout
        self.run_doc_mdex_test(
            "https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/706496/GCSE_Factsheet_employerFEHE_May_2018_.pdf",
            "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/706496/GCSE_Factsheet_employerFEHE_May_2018_.pdf",
            'https://www.gov.uk/government/publications?departments%5B%5D=department-for-education',
            36033, "GCSE new grading scale: factsheets")

        self.run_doc_mdex_test(
            "https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/704095/commercial-victimisation-survey-technical-report-2017.pdf",
            "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/704095/commercial-victimisation-survey-technical-report-2017.pdf",
            'https://www.gov.uk/government/publications?departments%5B%5D=home-office',
            35912, "Crime against businesses: findings from the 2017 Commercial Victimisation Survey")


        # Example of a non-publication
        self.run_doc_mdex_test(
            "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/397042/Agenda_and_papers_for_29_January_2015_board_meeting.pdf",
            "https://www.gov.uk/government/organisations/environment-agency/about/our-governance",
            'https://www.gov.uk/government/publications?departments[]=department-for-transport',
            None, "2015 board meetings")

        # Command and Act papers
        self.run_doc_mdex_test(
            'https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/594447/Local_government_finance_report.pdf',
            'https://www.gov.uk/government/publications/local-government-finance-report-2017-to-2018',
            'https://www.gov.uk/government/publications?departments[]=department-for-transport',
            35913, "Local Government Finance Report (England) 2017 to 2018")

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

        # These tests association-with-closest-heading logic:
        self.run_doc_mdex_test_extraction(
            "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/128968/competency-guidance.pdf",
            "https://www.gov.uk/government/organisations/department-for-work-pensions/about/recruitment",
            "https://www.gov.uk/government/organisations/department-for-work-pensions",
            "Guidance on writing competency statements for a job application")
        #self.run_doc_mdex_test_extraction(
        #    "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/421402/List_of_lawyers_in_Mexico.pdf",
        #    "https://www.gov.uk/government/world/organisations/british-embassy-mexico-city",
        #    "https://www.gov.uk/government/publications?departments[]=department-for-transport",
        #    "List of lawyers and interpreters")

        # the tests Target association:
        # - scottish parliament
        self.run_doc_mdex_test('http://www.parliament.scot/S4_EducationandCultureCommittee/BBC charter/BBCcallforviews.pdf',
                          'http://www.parliament.scot/help/92650.aspx',
                          'http://www.parliament.scot/',
                          36096, "BBC charter renewal - Call for views")

        # - Children's Commissioner
        self.run_doc_mdex_test(
            'https://www.childrenscommissioner.gov.uk/wp-content/uploads/2017/08/The-views-of-children-and-young-people-regarding-media-access-to-family-courts.pdf',
            'https://www.childrenscommissioner.gov.uk/publication/report-on-the-views-of-children-and-young-people-regarding-media-access-to-family-courts/',
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

        # gov.uk documnent discovered via other sites:
        self.run_doc_mdex_test(
            'https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/214379/WP77techapp.pdf',
            'http://www.ifs.org.uk/publications/8736',
            'http://www.ifs.org.uk/',
            36031, "Technical annexe")

        # This example is problematic becuase it's a gov.uk document without an 'up' relation to discover it's proper landing page.
        # Running separate crawls or more complete link-based document extraction and analysis would avoid this.
        # It's ALSO problematic because I think Huffintonpost may be blocking us!
        #self.run_doc_mdex_test(
        #    'https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/438143/analysis-of-the-airports-commission_s-consultation-responses.pdf',
        #    'http://www.huffingtonpost.co.uk/rob-gray/heathrow-expansion-building-runway_b_12634602.html?utm_hp_ref=heathrow-third-runway',
        #    'http://www.huffingtonpost.co.uk/',
        #    None, "We've Backed A New Heathrow Runway... Now We Need To Build It!")

        # OFFLINE so cannot be tested without access to the actual web archive!
        # - Local Government Association
        #self.run_doc_mdex_test(
        #    'http://www.local.gov.uk/documents/10180/5716319/LGA+DECC+energy+efficiency+221113.pdf/86a87aaf-8650-4ef3-969b-3aff0e50083e',
        #    'http://www.local.gov.uk/web/guest/media-releases/-/journal_content/56/10180/5716193/NEWS',
        #    'http://www.local.gov.uk/publications',
        #    36040,
        #    "LGA press release 30 November 2013")  # page title: "Allow councils to lead energy efficiency schemes, says LGA")

    def run_doc_mdex_test(self, url, lpu, src, tid, title):
        logger.info("\n\nLooking at document URL: %s" % url)
        doc = {}
        doc['document_url'] = url
        doc['landing_page_url'] = lpu
        targets = self.load_targets()
        doc = DocumentMDEx(targets, doc, src, null_if_no_target_found=False).mdex()
        logger.info(json.dumps(doc))
        if doc['target_id'] != tid:
            raise Exception("Target matching failed! %s v %s" % (doc['target_id'], tid))
        if doc.get('title', None) != title:
            raise Exception("Wrong title found for this document! '%s' v '%s'" % (doc['title'], title))

    def run_doc_mdex_test_extraction(self, url, lpu, src, title):

        logger.info("\n\nLooking at document URL: %s" % url)
        doc = {}
        doc['document_url'] = url
        doc['landing_page_url'] = lpu
        targets = self.load_targets()
        doc = DocumentMDEx(targets, doc, src, null_if_no_target_found=False).mdex()
        logger.info(json.dumps(doc))
        if doc.get('title', None) != title:
            raise Exception("Wrong title found for this document! '%s' v '%s'" % (doc['title'], title))


if __name__ == '__main__':
    #luigi.run(['scan.ScanForDocuments', '--date-interval', '2016-11-04-2016-11-10'])  # , '--local-scheduler'])
    luigi.run(['test.RunDocumentExtractionTests', '--local-scheduler'])
