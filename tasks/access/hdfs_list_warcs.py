import os
import re
import csv
import enum
import json
import gzip
import glob
import shutil
import pysolr
import logging
import datetime
import luigi
import luigi.contrib.hdfs
import luigi.contrib.webhdfs
from tasks.analyse.hdfs_analysis import UpdateWarcsDatabase
from tasks.common import state_file
from lib.webhdfs import webhdfs
from lib.targets import AccessTaskDBTarget, TaskTarget, TrackingDBStatusField

logger = logging.getLogger('luigi-interface')

"""
Tasks relating to using the list of HDFS content to update access systems.
"""


class ListWarcsForDateRange(luigi.Task):
    """
    Lists the WARCS with datestamps corresponding to a particular day. Defaults to last week until yesterday.
    Dates are inclusive, so setting start and end to the same date == one day
    """
    start_date = luigi.DateParameter(default=datetime.date.today() - datetime.timedelta(7))
    end_date = luigi.DateParameter(default=datetime.date.today() - datetime.timedelta(1))
    stream = luigi.Parameter(default='frequent')
    status_field = luigi.Parameter(default='cdx_index_ss')
    status_value = luigi.Parameter(default='data-heritrix')
    # Kind of data to list (usually WARCS)
    kind = luigi.Parameter(default='warcs')
    # Max number of items to return.
    limit = luigi.IntParameter(default=1000)
    # Specify the Tracking DB to use to manage state:
    tracking_db_url = luigi.Parameter(default=TrackingDBStatusField.DEFAULT_TRACKDB)
    # Specify a fine-grained run date so we can get fresh results
    run_date = luigi.DateMinuteParameter(default=datetime.datetime.now())

    task_namespace = 'access.list'

    def requires(self):
        return UpdateWarcsDatabase(trackdb=self.tracking_db_url)

    def run(self):

        # Query
        s = pysolr.Solr(url=self.tracking_db_url)
        q='kind_s:"%s" AND stream_s:"%s" AND timestamp_dt:[%sT00:00:00Z TO %sT23:59:59Z] AND -%s:"%s"' % (
            self.kind,
            self.stream,
            self.start_date.isoformat(),
            self.end_date.isoformat(),
            self.status_field,
            self.status_value
        )
        logger.info("Query = %s" % q)
        # Limit to no more than e.g. 1000 at once:
        params = {
            'rows': self.limit
        }
        result = s.search(q=q, **params)

        # Write out:
        with self.output().open('w') as f:
            for doc in result.docs:
                f.write(doc['file_path_s'])
                f.write('\n')

    def output(self):
        # Use the run_date as part of the output to make sure it's fresh.
        return TaskTarget('warc-file-list','-to-%s-as-of-%s.txt' % (self.start_date, self.run_date), self.end_date)


if __name__ == '__main__':
    import logging

    logging.getLogger().setLevel(logging.INFO)

    luigi.run(['access.list.ListWarcsForDateRange'])
