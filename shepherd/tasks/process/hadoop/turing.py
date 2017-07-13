import os
import json
import zlib
import luigi
import luigi.format
import luigi.contrib.hdfs
import StringIO
import datetime
from azure.storage.blob import BlockBlobService
from hasher import ListAllFilesOnHDFS
from shepherd.tasks.common import state_file
from shepherd.tasks.common import logger


class UploadToAzure(luigi.Task):
    path = luigi.Parameter()
    container = luigi.Parameter(default='ukwebarchive')
    prefix = luigi.Parameter(default='jisc-uk-web-domain-dataset-1996-2013')

    block_blob_service = BlockBlobService(
        account_name=os.environ.get('AZURE_ACCOUNT_NAME'),
        account_key=os.environ.get('AZURE_ACCOUNT_KEY')
    )

    def full_path(self):
        return "%s/%s" % (self.prefix, self.path.lstrip('/'))

    def complete(self):
        source = luigi.contrib.hdfs.HdfsTarget(path=self.path)
        size = source.fs.client.status(source.path)['length']
        # Check the path exists and is the right size:
        if self.block_blob_service.exists(self.container, self.full_path()):
            props = self.block_blob_service.get_blob_properties(self.container, self.full_path())
            if props.properties.content_length == size:
                return True
        # Wrong...
        return False

    def run(self):
        source = luigi.contrib.hdfs.HdfsTarget(path=self.path)
        with source.fs.client.read(source.path) as inf:
            self.block_blob_service.create_blob_from_stream(self.container, self.full_path(), inf, max_connections=1)


class ListFilesToUploadToAzure(luigi.Task):
    """
    Takes the full WARC list and filters UKWA content by folder:
    """
    date = luigi.DateParameter(default=datetime.date.today())
    path_match = luigi.Parameter(default='/ia/2011-201304/')

    def requires(self):
        return ListAllFilesOnHDFS(self.date)

    def output(self):
        return state_file(self.date, 'hdfs', 'turing-uploaded-file-list.jsonl')

    def run(self):
        filenames = {}
        with self.input().open('r') as reader:
            for line in reader:
                item = json.loads(line.strip())
                if item['filename'].startswith(self.path_match):
                    logger.info("Found matching item: '%s'" % item['filename'])


if __name__ == '__main__':
    luigi.run(['ListFilesToUploadToAzure'])
    #luigi.run(['ListFilesToUploadToAzure', '--local-scheduler' , '--path-match' , '/user/root/input/hadoop'])
    #luigi.run(['UploadToAzure', '--path', '/ia/2011-201304/part-01/warcs/DOTUK-HISTORICAL-2011-201304-WARCS-PART-00044-601503-000001.warc.gz'])
