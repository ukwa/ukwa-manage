# Overall purpose here is to import data from third parties

import os
import pysftp
import logging
import datetime
import calendar
from lib.store.webhdfs import WebHDFSStore

logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(levelname)s - %(name)s - %(message)s')

logger = logging.getLogger(__name__)

def add_months(date, months):
    month = date.month + months

    # Calculate the year and month:
    year = date.year
    while month < 1:
        month += 12
        year -= 1
    while month > 12:
        month -= 12
        year += 1

    # Calculate the day
    day = date.day
    last_day_of_month = calendar.monthrange(year, month)[1]
    if day > last_day_of_month:
        day = last_day_of_month

    new_date = datetime.date(year, month, day)
    return new_date

def ingest_from_nominet(w):
    #: the FTP server
    NOM_HOST = os.environ['NOM_HOST']
    #: the username
    NOM_USER = os.environ['NOM_USER']
    #: the password
    NOM_PWD = os.environ['NOM_PWD']

    # Connect, without host key verification
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    logger.info("Connecting to %s@%s..." % (NOM_USER, NOM_HOST))
    with pysftp.Connection(NOM_HOST, username=NOM_USER, password=NOM_PWD, cnopts=cnopts) as sftp:
        # Iterate over recent months:
        file_date = add_months(datetime.date.today(), -4)
        next_date = add_months(datetime.date.today(), 1)
        while file_date < next_date:
            # Construct the filename and target HDFS path:
            filename = 'ukdata-%s01.zip' % file_date.strftime('%Y%m')
            hdfsfile = "/1_data/nominet/%s" % filename
            ftpfile = "/ukdata/%s" % filename
            logger.info("Attempting to download '%s' via SFTP..." % ftpfile)
            if sftp.exists(ftpfile):
                sftp.get(ftpfile, filename)
                logger.warn("Uploading '%s' to HDFS path '%s'..." % (filename, hdfsfile))
                w.put(filename, hdfsfile)
            else:
                logger.warn("No file '%s' found!" % ftpfile)
            # Try the next month:
            file_date = add_months(file_date, 1)

if __name__ == '__main__':
    w = WebHDFSStore('h3', user_override='ingest')
    ingest_from_nominet(w)
