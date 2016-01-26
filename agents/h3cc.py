#!/usr/bin/env python
# encoding: utf-8
'''
agents.h3cc -- Heritrix3 Crawl Controller

agents.h3cc is a tool for controlling Heritrix3, performing basic operations like starting 
and stopping crawls, reporting on crawler status, or updating crawler configuration.

@author:     Andrew Jackson

@copyright:  2016 The British Library.

@license:    Apache 2.0

@contact:    Andrew.Jackson@bl.uk
@deffield    updated: 2016-01-16
'''

import sys
import os
import logging
import heritrix
import requests

# Prevent cert warnings
requests.packages.urllib3.disable_warnings()

from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter

__all__ = []
__version__ = 0.1
__date__ = '2016-01-16'
__updated__ = '2016-01-16'

# Set up a logging handler:
handler = logging.StreamHandler()
#handler = logging.StreamHandler(sys.stdout) # To use stdout rather than the default stderr
formatter = logging.Formatter( "[%(asctime)s] %(levelname)s %(filename)s.%(funcName)s@%(lineno)d: %(message)s" )
handler.setFormatter( formatter ) 

# Attach to root logger
logging.root.addHandler( handler )

# Set default logging output for all modules.
logging.root.setLevel( logging.WARNING )

# Set logging for this module and keep the reference handy:
logger = logging.getLogger( __name__ )
logger.setLevel( logging.INFO )

def main(argv=None):
    '''Command line options.'''

    if argv is None:
        argv = sys.argv
    else:
        sys.argv.extend(argv)

    program_name = os.path.basename(sys.argv[0])
    program_version = "v%s" % __version__
    program_build_date = str(__updated__)
    program_version_message = '%%(prog)s %s (%s)' % (program_version, program_build_date)
    program_shortdesc = __import__('__main__').__doc__.split("\n")[1]
    program_license = '''%s

  Created by Andrew Jackson on %s.
  Copyright 2016 The British Library.

  Licensed under the Apache License 2.0
  http://www.apache.org/licenses/LICENSE-2.0

  Distributed on an "AS IS" basis without warranties
  or conditions of any kind, either express or implied.

USAGE
''' % (program_shortdesc, str(__date__))

    try:
        # Setup argument parser
        parser = ArgumentParser(description=program_license, formatter_class=RawDescriptionHelpFormatter)
        parser.add_argument("-v", "--verbose", dest="verbose", action="count", help="set verbosity level [default: %(default)s]")
        parser.add_argument('-V', '--version', action='version', version=program_version_message)
        parser.add_argument('-j', '--job', dest='job', default='frequent',
                            help="Name of job to operate upon. [default: %(default)s]")
        parser.add_argument(dest="command", help="Command to carry out, 'list', 'status', 'info-xml'. [default: %(default)s]", metavar="command")

        # Process arguments
        args = parser.parse_args()

        # Up the logging
        verbose = args.verbose
        if verbose > 0:
            logger.setLevel( logging.DEBUG )

        # talk to h3:
        ha = heritrix.API(host="https://%s:%s/engine" % ('192.168.99.100', '8443'), user='heritrix', passwd='heritrix', verbose=True, verify=False)
        job = args.job
        
        # Commands:
        command = args.command
        if command == "list":
            print ha.listjobs()
        elif command == "status":
            print ha.status(job)
        elif command == "info-xml":
            print ha._job_action("", job).text
        else:
            logger.error("Can't understand command '%s'" % command)

        return 0
    except KeyboardInterrupt:
        ### handle keyboard interrupt ###
        return 0
    except Exception, e:
        indent = len(program_name) * " "
        sys.stderr.write(program_name + ": " + repr(e) + "\n")
        sys.stderr.write(indent + "  for help use --help")
        return 2

if __name__ == "__main__":
    sys.exit(main())
    