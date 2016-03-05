#!/usr/bin/env python

"""Given a series of 'jobnames' of the form 'job/launchid', will create a METS
file and form a Bagit folder for said METS."""

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
from mets import Mets
from datetime import datetime
from xml.dom.minidom import parseString
from pywebhdfs.webhdfs import PyWebHdfsClient

logger = logging.getLogger( __name__ )
logging.root.setLevel( logging.DEBUG )

class SipCreator:
    def __init__( self, jobs, jobname, warcs=None, viral=None, logs=None, start_date=None, args=None ):
        """Sets up APIs."""
        self.dummy = args.dummy_run
        self.jobs = jobs
        self.jobname = jobname
        self.warcs = warcs
        self.viral = viral
        self.logs = logs
        self.startdate = start_date
        self.BAGIT_CONTACT_NAME="Roger G. Coram"
        self.BAGIT_CONTACT_EMAIL="roger.coram@bl.uk"
        self.BAGIT_DESCRIPTION="LD Crawl: "
        self.ARK_URL="http://pii.ad.bl.uk/pii/vdc?arks="
        self.ARK_PREFIX="ark:/81055/vdc_100022535899.0x"
        self.NUM_THREADS=10
        

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

    def verifyFileLocations( self ):
        """Checks that the configured file locations and job paths are sane."""
        verified = os.path.exists( self.LOCAL_ROOT ) and \
        os.path.exists( self.WARC_ROOT ) and  \
        os.path.exists( self.LOG_ROOT ) and  \
        os.path.exists( self.VIRAL_ROOT ) and  \
        os.path.exists( self.JOBS_ROOT )
        for job in self.jobs:
            verified = verified and \
            os.path.exists( "%s/%s" % ( self.JOBS_ROOT, job ) ) and  \
            os.path.exists( "%s/%s" % ( self.WARC_ROOT, job ) ) and  \
            os.path.exists( "%s/%s" % ( self.LOG_ROOT, job ) )
        return verified
       
    def verifySetup(self):
        return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser( description="Create METS files." )
    parser.add_argument( "jobs", metavar="J", type=str, nargs="+", help="Heritrix job name" )
    parser.add_argument( "-d", dest="dummy", action="store_true" )
    parser.add_argument( "-w", dest="warcs", help="File containing list of WARC paths." )
    parser.add_argument( "-v", dest="viral", help="File containing list of viral WARC paths." )
    parser.add_argument( "-l", dest="logs", help="File containing list of log paths." )
    args = parser.parse_args()

    jobname = datetime.now().strftime( "%Y%m%d%H%M%S" )
    sip = SipCreator( args.jobs, jobname=jobname, args=args, warcs=args.warcs, viral=args.viral, logs=args.logs )
    if sip.verifySetup():
        sip.processJobs()
        sip.createMets()
        if not os.path.isdir("%s/%s" % (args.output_root, jobname)):
            os.makedirs("%s/%s/" % (args.output_root, jobname))
        with open( "%s/%s/%s.xml" % ( args.output_root, jobname, jobname ), "wb" ) as o:
            sip.writeMets( o )
        sip.bagit( "%s/%s" % ( args.output_root, jobname ) )

