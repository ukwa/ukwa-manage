#!/usr/bin/env python

"""Given a series of 'jobnames' of the form 'job/launchid', will create a METS
file and form a Bagit folder for said METS."""

from __future__ import absolute_import

import re
import os
import sys
import glob
import json
import bagit
import logging
import argparse
import requests
import subprocess
import shutil
import hdfs
from crawl.sip.mets import Mets
from datetime import datetime
from xml.dom.minidom import parseString

# import the Celery app context
from crawl.celery import app
from crawl.celery import cfg

# import the Celery log getter and use it
from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


class SipCreator:
    def __init__( self, jobs, jobname=datetime.now().strftime( "%Y%m%d%H%M%S" ), warcs=None, viral=None, logs=None, start_date=None, dummy_run=False ):
        """Sets up fields."""
        self.hdfs =  hdfs.InsecureClient(cfg.get('hdfs','url'), user=cfg.get('hdfs','user'))
        self.dummy = dummy_run
        self.overwrite = False
        self.jobs = jobs
        self.jobname = jobname
        self.warcs = warcs
        self.viral = viral
        self.logs = logs
        self.startdate = start_date
        self.BAGIT_CONTACT_NAME="Andrew N. Jackson"
        self.BAGIT_CONTACT_EMAIL="andrew.jackson@bl.uk"
        self.BAGIT_DESCRIPTION="LD Crawl: "
        self.ARK_URL="http://pii.ad.bl.uk/pii/vdc?arks="
        self.ARK_PREFIX="ark:/81055/vdc_100022535899.0x"
        # And create:
        logger.info("Processing job files...")
        self.processJobs()
        logger.info("Generating METS...")
        self.createMets()

    def processJobs( self ):
        """All WARCs and logs associated with jobs, optionally loaded from files."""
        if not isinstance(self.warcs,list):
            with open(self.warcs, "r") as i:
                self.warcs = [w.strip() for w in i]

        if not isinstance(self.viral,list):
            with open(self.viral, "r") as i:
                self.viral = [v.strip() for v in i]

        if not isinstance(self.logs,list):
            with open(self.logs, "r") as i:
                self.logs = [l.strip() for l in i]

        if self.startdate is None and self.dummy:
            self.startdate = datetime.now().isoformat()

        if( self.dummy ):
            logger.info( "Getting dummy ARK identifiers..." )
            self.getDummyIdentifiers( len( self.warcs ) + len( self.viral ) + len( self.logs ) )
        else:
            logger.info( "Getting ARK identifiers..." )
            self.getIdentifiers( len( self.warcs ) + len( self.viral ) + len( self.logs ) )

    def createMets( self ):
        """Creates the Mets object."""
        logger.info( "Building METS..." )
        self.mets = Mets( self.startdate, self.warcs, self.viral, self.logs, self.identifiers )

    def writeMets( self, output=sys.stdout ):
        """Writes the METS XML to a file handle."""
        output.write( self.mets.getXml() )

    def bagit( self, directory, metadata=None ):
        """Creates a Bagit, if needs be with default metadata."""
        if metadata is None:
            metadata = { "Contact-Name": self.BAGIT_CONTACT_NAME, "Contact-Email": self.BAGIT_CONTACT_EMAIL, "Timestamp": datetime.now().strftime( "%Y-%m-%dT%H:%M:%SZ" ), "Description": self.BAGIT_DESCRIPTION + ";".join( self.jobs ) }
        bagit.make_bag( directory, metadata )


    def getDummyIdentifiers( self, num ):
        """Provides a series of 'dummy' identifiers for testing purposes."""
        self.identifiers = []
        for i in range( num ):
            self.identifiers.append( "%s%s" % ( self.ARK_PREFIX, "{0:06d}".format( i ) ) )

    def getIdentifiers( self, num ):
        """Retrieves 'num' ARK identifiers from a webservice; alternatively calls an
        alternate method to provide 'dummy' identifiers."""
        self.identifiers = []
        if( self.dummy ):
            return self.getDummyIdentifiers( num )
        try:
            url = "%s%s" % ( self.ARK_URL, str( num ) )
            logger.debug( "Requesting %s ARKS: %s" % ( num, url ) )
            response = requests.post( url )
            data = response.content
        except Exception as e:
            logger.error( "Could not obtain ARKs: %s" % str( e ) )
            raise Exception( "Could not obtain ARKs: %s" % str( e ) )
        xml = parseString( data )
        for ark in xml.getElementsByTagName( "ark" ):
            self.identifiers.append( ark.firstChild.wholeText )
        if( len( self.identifiers ) != num ):
            raise Exception( "Problem parsing ARKs; %s, %s, %s" % ( self.jobs, self.identifiers, data ) )

    def verifySetup(self):
        return True


    def create_sip(self, sip_dir):
        """Creates a SIP and returns the path to the folder containing the METS."""
        if self.verifySetup():
            if not os.path.exists(sip_dir):
                os.makedirs(sip_dir)
            else:
                raise Exception("SIP directory already exists: %s" % sip_dir)
            with open("%s/%s.xml" % (sip_dir, self.jobname.replace('/','_')), "wb") as o:
                self.writeMets(o)
            self.bagit(sip_dir)
        else:
            raise Exception("Could not verify SIP for %s" % sip_dir)
        return sip_dir


    def copy_sip_to_hdfs(self, sip_dir, hdfs_sip_path):
        """
        Creates a tarball of a SIP and copies to HDFS.
        """
        # Check if it's already there:
        hdfs_sip_tgz = "%s.tar.gz" % hdfs_sip_path
        status = self.hdfs.status(hdfs_sip_tgz, strict=False)
        if status and not self.overwrite:
            raise Exception("SIP already exists in HDFS: %s" % hdfs_sip_tgz)
        # Build the TGZ
        gztar = shutil.make_archive(base_name=sip_dir, format="gztar", root_dir=os.path.dirname(sip_dir),
                                    base_dir=os.path.basename(sip_dir))
        logger.info("Copying %s to HDFS..." % gztar)
        with open(gztar,'r') as f:
            self.hdfs.write(data=f, hdfs_path=hdfs_sip_tgz, overwrite=False)
        os.remove(gztar)
        logger.info("Done.")
        return hdfs_sip_tgz

def main():
    parser = argparse.ArgumentParser( description="Create METS files." )
    parser.add_argument( "jobs", metavar="J", type=str, nargs="+", help="Heritrix job name" )
    parser.add_argument( "-d", dest="dummy", action="store_true" )
    parser.add_argument( "-w", dest="warcs", help="File containing list of WARC paths." )
    parser.add_argument( "-v", dest="viral", help="File containing list of viral WARC paths." )
    parser.add_argument( "-l", dest="logs", help="File containing list of log paths." )
    parser.add_argument( "-o", dest="output_root", help="Where to put the resulting SIP" )
    args = parser.parse_args()

    sip = SipCreator( args.jobs, warcs=args.warcs, viral=args.viral, logs=args.logs, dummy_run=args.dummy )
    sip_dir = "%s/%s" % ( args.output_root, sip.jobname )
    sip.create_sip(sip_dir)
    sip.copy_sip_to_hdfs(sip_dir, sip_dir)

if __name__ == "__main__":
    main()

