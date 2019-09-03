import os
import re
import luigi
import shutil
import datetime
from lib.targets import IngestTaskDBTarget

class MoveWarcProxFiles(luigi.Task):

    task_namespace = 'ingest'
    date = luigi.DateHourParameter(default=datetime.datetime.today())
    prefix = luigi.Parameter(default="/mnt/gluster/fc")

    def output(self):
        return IngestTaskDBTarget('mv-warcprox-files', self.task_id)
    
    def run(self):
        # Expected filenaming:
        p = re.compile("BL-....-WEBRENDER-([a-z\-0-9]+)-([0-9]{14})-([a-z\-0-9]+)\.warc\.gz")
        # List all matching files in source directory:
        webrender_path = os.path.join(self.prefix, 'heritrix/wren/')
        for file_path in os.listdir(webrender_path):
            if file_path.endswith('.warc.gz'):
                file_name = os.path.basename(file_path)
                print(file_name)
                matches = p.search(file_name)
                if matches:
                    destination_folder_path = "%s/heritrix/output/%s/%s/warcs" %( self.prefix, matches.group(1), matches.group(2))
                    if not os.path.exists(destination_folder_path):
                        raise Exception("Expected destination folder does not exist! :: %s" % destination_folder_path)
                    if not os.path.isdir(destination_folder_path):
                        raise Exception("Expected destination folder is not a folder! :: %s" % destination_folder_path)
                    source_file_path = os.path.join(webrender_path, file_name)
                    destination_file_path = os.path.join(destination_folder_path, file_name)
                    if os.path.exists(destination_file_path):
                        raise Exception("Destination file already exists! :: %s" % destination_file_path)
                    shutil.move( source_file_path, destination_file_path )

        # Record that all went well:
        self.output().touch()

if __name__ == '__main__':
    luigi.run(['ingest.MoveWarcProxFiles', '--local-scheduler'])
