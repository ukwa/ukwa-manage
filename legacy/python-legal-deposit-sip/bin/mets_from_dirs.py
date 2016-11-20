#!/usr/bin/env python
"""
Creates a SIP from a list of directories. The script will include all "*.warc.gz" files therein, presuming them to
be non-viral:

    mets_from_dirs.py --directories "/heritrix/output/images" --prefix "images"


"""

import os
import sip
import argparse
import tempfile
from glob import glob
from datetime import datetime

parser = argparse.ArgumentParser(description="Generate SIP from directories.")
parser.add_argument("-d", "--directories", dest="directories", type=str, nargs="+", help="Directories", required=True)
parser.add_argument("-p", "--prefix", dest="prefix", type=str, required=True)
args = parser.parse_args()

now = datetime.now().strftime("%Y%m%d%H%M%S")
jobname = "%s-%s/%s" % (args.prefix, now, now)
sip_dir = "%s/%s" % (settings.SIP_ROOT, jobname)

with tempfile.NamedTemporaryFile(delete=False) as temp_warcs:
    warcs = []
    for dir in ["/heritrix/output/images"]:
    #for dir in args.directories:
        warcs += glob("%s/*.warc.gz" %(dir))
    warcs = warcs[:10]
    temp_warcs.write("\n".join(warcs))

s = sip.SipCreator(jobs=[jobname], jobname=jobname, dummy=True, warcs=temp_warcs.name, viral="/dev/null", logs="/dev/null")
if s.verifySetup():
    s.processJobs()
    s.createMets()
    filename = os.path.basename(jobname)
    os.makedirs(sip_dir)
    with open("%s/%s.xml" % (sip_dir, filename), "wb") as o:
        s.writeMets(o)
    s.bagit(sip_dir)
else:
    raise Exception("Could not verify SIP for %s" % job)

os.unlink(temp_warcs)

