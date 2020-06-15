# Overall purpose here is to import data from third parties

import os
import pysftp
import logging
import datetime
import calendar
from lib.store.webhdfs import WebHDFSStore

logger = logging.getLogger(__name__)

#: the FTP server
NOM_HOST = os.environ['NOM_HOST']
#: the username
NOM_USER = os.environ['NOM_USER']
#: the password
NOM_PWD = os.environ['NOM_PWD']

def add_months(date, months):
    months_count = date.month + months

    # Calculate the year
    year = date.year + int(months_count / 12)

    # Calculate the month
    month = (months_count % 12)
    if month == 0:
        month = 12

    # Calculate the day
    day = date.day
    last_day_of_month = calendar.monthrange(year, month)[1]
    if day > last_day_of_month:
        day = last_day_of_month

    new_date = datetime.date(year, month, day)
    return new_date

def ingest_from_nominet(w):
    file_date = add_months(datetime.date.today(), -4)
    next_date = add_months(datetime.date.today(), 1)
    while file_date < next_date:
        file = 'domains.%s.csv.gz' % file_date.strftime('%Y%m')
        hdfsfile = "/1_data/nominet/domains.%s.csv.gz" % file_date.strftime('%Y%m')
        logger.warn("Attempting to SFTP %s -> %s" % (file, hdfsfile))
        # Connect, without host key verification
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        with pysftp.Connection(NOM_HOST, username=NOM_USER, password=NOM_PWD, cnopts=cnopts) as sftp:
            sftp.get(file)
            w.put(file, hdfsfile)
        file_date = add_months(file_date, 1)

if __name__ == '__main__':
    w = WebHDFSStore()
    ingest_from_nominet(w)
