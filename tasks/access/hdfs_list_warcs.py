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
    Lists the WARCS with datestamps corresponding to a particular day. Defaults to yesterday.
    """
    start_date = luigi.DateParameter(default=datetime.date.today() - datetime.timedelta(1))
    end_date = luigi.DateParameter(default=datetime.date.today())
    stream = luigi.Parameter(default='frequent')
    status_field = luigi.Parameter(default='cdx_status_ss')
    status_value = luigi.Parameter(default='data-heritrix')

    task_namespace = 'access.list'

    def output(self):
        return TaskTarget('warc-file-list','to-%s.json' % self.end_date, self.start_date)


    def run(self):
        # Query
        s = pysolr.Solr(url=TrackingDBStatusField.DEFAULT_TRACKDB)
        q='stream_s:"%s" AND timestamp_dt:[%sZ TO %sZ] AND -%s:"%s"' % (
            self.stream,
            self.start_date.isoformat(),
            self.end_date.isoformat(),
            self.status_field,
            self.status_value
        )
        logger.info("Query = %s" % q)
        result = s.search(q=q)

        # Write out:
        with self.output().open('w') as f:
            for doc in result.docs:
                f.write(json.dumps(doc))
                f.write('\n')


if __name__ == '__main__':
    import logging

    logging.getLogger().setLevel(logging.INFO)

    luigi.run(['access.list.ListWarcsForDateRange'])
