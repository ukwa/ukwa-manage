#!/usr/bin/env python

import os
import sys
import json
import pika
import time
import logging
import argparse
from urlparse import urlparse
import requests
from requests.utils import quote
import xml.dom.minidom

from crawl.w3act import w3act
from document_mdex import DocumentMDEx

# Set up a logging handler:
handler = logging.StreamHandler()
#handler = logging.StreamHandler(sys.stdout) # To use stdout rather than the default stderr
formatter = logging.Formatter( "[%(asctime)s] %(levelname)s %(filename)s.%(funcName)s: %(message)s" )
handler.setFormatter( formatter ) 

# Replace root logger
logging.root.handlers = []
logging.root.addHandler( handler )
logging.root.setLevel( logging.DEBUG )

# Set default logging output for all modules.
logging.getLogger('requests').setLevel( logging.WARNING )
logging.getLogger('pika').setLevel( logging.WARNING )

# Set logging for this module and keep the reference handy:
logger = logging.getLogger(__name__)

def run_doc_mdex_test(url,lpu,src,tid,title):
    logger.info("Looking at document URL: %s" % url)
    doc = {}
    doc['document_url'] = url
    doc['landing_page_url'] = lpu
    doc = DocumentMDEx(act, doc, src, null_if_no_target_found=False).mdex()
    logger.info(json.dumps(doc))
    if doc['target_id'] != tid:
        logger.error("Target matching failed! %s v %s" % (doc['target_id'], tid))
        sys.exit()
    if doc.get('title',None) != title:
        logger.error("Wrong title found for this document! '%s' v '%s'" % (doc['title'], title))
        sys.exit()

def run_doc_mdex_test_extraction(url,lpu,src,title):
    logger.info("Looking at document URL: %s" % url)
    doc = {}
    doc['document_url'] = url
    doc['landing_page_url'] = lpu
    doc = DocumentMDEx(act, doc, src, null_if_no_target_found=False).mdex()
    logger.info(json.dumps(doc))
    if doc.get('title',None) != title:
        logger.error("Wrong title found for this document! '%s' v '%s'" % (doc['title'], title))
        sys.exit()

if __name__ == "__main__":
    parser = argparse.ArgumentParser('Test document extraction and target association.')
    parser.add_argument('-w', '--w3act-url', dest='w3act_url', 
                    type=str, default="http://localhost:9000/act/", 
                    help="W3ACT endpoint to use [default: %(default)s]" )
    parser.add_argument('-u', '--w3act-user', dest='w3act_user', 
                    type=str, default="wa-sysadm@bl.uk", 
                    help="W3ACT user email to login with [default: %(default)s]" )
    parser.add_argument('-p', '--w3act-pw', dest='w3act_pw', 
                    type=str, default="sysAdmin", 
                    help="W3ACT user password [default: %(default)s]" )
    parser.add_argument('-W', '--wb-url', dest='wb_url', 
                    type=str, default="http://localhost:8080/wayback", 
                    help="Wayback endpoint to check URL availability [default: %(default)s]" )
    
    args = parser.parse_args()

    # Set up connection to ACT:
    act = w3act.w3act(args.w3act_url,args.w3act_user,args.w3act_pw)


    # Extraction tests:
    run_doc_mdex_test_extraction(
        "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/421402/List_of_lawyers_in_Mexico.pdf",
        "https://www.gov.uk/government/world/organisations/british-embassy-mexico-city",
        "https://www.gov.uk/government/publications?departments[]=department-for-transport",
        "List of lawyers and interpreters")

    run_doc_mdex_test_extraction(
        "https://www.euromod.ac.uk/sites/default/files/working-papers/em2-01.pdf",
        "https://www.euromod.ac.uk/publications/date/2001/type/EUROMOD%20Working%20Paper%20Series",
        "https://www.euromod.ac.uk/", "Towards a multi purpose framework for tax benefit microsimulation")

    run_doc_mdex_test_extraction(
        "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/128968/competency-guidance.pdf",
        "https://www.gov.uk/government/organisations/department-for-work-pensions/about/recruitment",
        "https://www.gov.uk/government/organisations/department-for-work-pensions", "Guidance on writing competency statements for a job application")

    run_doc_mdex_test_extraction(
        "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/421402/List_of_lawyers_in_Mexico.pdf",
        "https://www.gov.uk/government/world/organisations/british-embassy-mexico-city",
        "https://www.gov.uk/government/publications?departments[]=department-for-transport",
        "List of lawyers and interpreters")

    # the tests Target association:
    # - scottish parliament
    run_doc_mdex_test('http://www.parliament.scot/S4_EducationandCultureCommittee/BBC charter/BBCcallforviews.pdf', 
        'http://www.parliament.scot/help/92650.aspx', 
        'http://www.parliament.scot/', 
        36096, "BBC charter renewal - Call for views")

    # - Children's Commissioner
    run_doc_mdex_test('http://www.childrenscommissioner.gov.uk/sites/default/files/publications/The%20views%20of%20children%20and%20young%20people%20regarding%20media%20access%20to%20family%20courts.pdf',
        'http://www.childrenscommissioner.gov.uk/publications/report-views-children-and-young-people-regarding-media-access-family-courts',
        'http://www.childrenscommissioner.gov.uk/publications', 
        36039, "Report on the views of children and young people regarding media access to family courts")

    # - ONS
    run_doc_mdex_test('https://www.ons.gov.uk/peoplepopulationandcommunity/birthsdeathsandmarriages/ageing/articles/characteristicsofolderpeople/2013-12-06/pdf',
        'http://www.ons.gov.uk/peoplepopulationandcommunity/birthsdeathsandmarriages/ageing/articles/characteristicsofolderpeople/2013-12-06',
        '',
        36037,"Characteristics of Older People: What does the 2011 Census tell us about the \"oldest old\" living in England & Wales?")

    # - Notts CAMRA
    run_doc_mdex_test('https://www.webarchive.org.uk/act-ddb/wayback/20160514170533/http://www.nottinghamcamra.org/festivals_720_2797277680.pdf',
        'http://www.nottinghamcamra.org/festivals.php',
        'http://nottinghamcamra.org',
        35989, "Beer Festivals")

    # - Local Government Association
    run_doc_mdex_test('http://www.local.gov.uk/documents/10180/5716319/LGA+DECC+energy+efficiency+221113.pdf/86a87aaf-8650-4ef3-969b-3aff0e50083e',
        'http://www.local.gov.uk/web/guest/media-releases/-/journal_content/56/10180/5716193/NEWS',
        'http://www.local.gov.uk/publications',
        36040, "Allow councils to lead energy efficiency schemes, says LGA")

    # - DCMS
    run_doc_mdex_test('https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/522511/Research_to_explore_public_views_about_the_BBC_-_Wave_1_data_tables.pdf',
        'https://www.gov.uk/government/publications/research-to-explore-public-views-about-the-bbc', 
        'https://www.gov.uk/government/publications?departments%5B%5D=department-for-culture-media-sport',
        36035, "Research to explore public views about the BBC - Data Tables Wave 1")

    # - ifs.org.uk
    run_doc_mdex_test('http://www.ifs.org.uk/uploads/cemmap/wps/cwp721515.pdf',
                    'http://www.ifs.org.uk/publications/8080','http://www.ifs.org.uk',
                    35915,"Identifying effects of multivalued treatments")
    run_doc_mdex_test('http://www.ifs.org.uk/uploads/publications/bns/BN179.pdf',
                    'http://www.ifs.org.uk/publications/8049','http://www.ifs.org.uk',
                    35915,"Funding the English & Welsh police service: from boom to bust?")

    # - gov.uk
    run_doc_mdex_test('https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/507081/2904936_Bean_Review_Web_Accessible.pdf',
                    'https://www.gov.uk/government/publications/independent-review-of-uk-economic-statistics-final-report',
                    'https://www.gov.uk/publications',
                    35909,"Independent review of UK economic statistics: final report")
    run_doc_mdex_test('https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/246770/0121.pdf',
                    'https://www.gov.uk/government/publications/met-office-annual-report-and-accounts-2012-to-2013', 
                    'https://www.gov.uk/',
                    35913,"Met Office annual report and accounts 2012/13 - Full Text")
    run_doc_mdex_test('https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/497536/rtfo-year-8-report-2.pdf',
                    'https://www.gov.uk/government/statistics/biofuel-statistics-year-8-2015-to-2016-report-2', 'https://www.gov.uk/',
                    35846,"Renewable Transport Fuel Obligation statistics: year 8, report 2")
    run_doc_mdex_test('https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/495227/harbour-closure-orders-consultation-summary-responses.pdf',
                    'https://www.gov.uk/government/consultations/harbour-closure-and-pilotage-function-removal-orders-draft-guidance', 'https://www.gov.uk/',
                    35846,"Guidance on harbour closure orders and pilotage function removal orders: summary of responses")
