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
    """
    start_date = luigi.DateParameter(default=datetime.date.today() - datetime.timedelta(7))
    end_date = luigi.DateParameter(default=datetime.date.today() - datetime.timedelta(1))
    stream = luigi.Parameter(default='frequent')
    status_field = luigi.Parameter(default='cdx_index_ss')
    status_value = luigi.Parameter(default='data-heritrix')
    limit = luigi.IntParameter(default=1000) # Max number of items to return.

    task_namespace = 'access.list'

    def output(self):
        return TaskTarget('warc-file-list','to-%s.json' % self.end_date, self.start_date)


    def run(self):

        # Query
        s = pysolr.Solr(url=TrackingDBStatusField.DEFAULT_TRACKDB)
        q='kind_s:"warcs" AND stream_s:"%s" AND timestamp_dt:[%sT00:00:00Z TO %sT23:59:59Z] AND -%s:"%s"' % (
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


if __name__ == '__main__':
    import logging

    logging.getLogger().setLevel(logging.INFO)

    luigi.run(['access.list.ListWarcsForDateRange'])
