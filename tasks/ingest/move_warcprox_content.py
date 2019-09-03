import os
import re
import luigi
import datetime
from lib.targets import IngestTaskDBTarget

class MoveWarcProxFiles(luigi.Task):

    task_namespace = 'ingest'
    date = luigi.DateHourParameter(default=datetime.datetime.today())
    prefix = luigi.Parameter(default="/mnt/gluster/fc")

    def output(self):
        return IngestTaskDBTarget('mv-warcprox-files', self.task_id, kind='ingest')


    def run(self):
        # Expected filenaming:
        p = re.compile("BL-....-WEBRENDER-/([a-z\-0-9]+)-([0-9]{14})-([a-z\-0-9]+)\.warc\.gz")
        # List all matching files in source directory:
        webrender_path = os.path.join(self.prefix, './heritrix/wren/')
        for file_path in os.listdir(webrender_path):
            if file_path.endswith('.warc.gz'):
                file_name = os.path.basename(file_path)
                matches = p.search(file_name)
                if len(matches) > 0:
                    print( "%s/heritrix/output/%s/%s/warcs" %( self.prefix, matches.group(1), matches.group(2) ))


        # Record that all went well:
        self.output().touch()

