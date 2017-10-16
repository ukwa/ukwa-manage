import os
import json
import zlib
import luigi
import luigi.format
import luigi.contrib.hdfs
import StringIO
import datetime
from azure.storage.blob import BlockBlobService
from ukwa.tasks.hadoop.hdfs import ListAllFilesOnHDFS
from ukwa.tasks.common import state_file
from ukwa.tasks.common import logger


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


class ListFilesToUploadToAzure(luigi.WrapperTask):
    """
    Takes the full WARC list and filters UKWA content by folder.

    Fixed date and path as we want to sync up a fixed set of files.
    """
    date = luigi.DateParameter(default=datetime.datetime.strptime('2017-10-13', '%Y-%m-%d'))
    path_match = luigi.Parameter(default='/ia/2011-201304/part-02/')

    def requires(self):
        file_list = ListAllFilesOnHDFS(self.date).output()
        with file_list.open('r') as reader:
            for line in reader:
                item = json.loads(line.strip())
                if item['filename'].startswith(self.path_match):
                    logger.info("Found matching item: '%s'" % item['filename'])
                    yield UploadToAzure(item['filename'])

    def output(self):
        return state_file(self.date, 'hdfs', 'turing-upload-file-list.tsv')

    def run(self):
        with self.output().open('w') as f:
            f.write("COMPLETED\t%s" % datetime.date.today())



if __name__ == '__main__':
    luigi.run(['ListFilesToUploadToAzure', '--workers', '10'])
    #luigi.run(['ListFilesToUploadToAzure', '--local-scheduler' , '--path-match' , '/user/root/input/hadoop'])
    #luigi.run(['UploadToAzure', '--path', '/ia/2011-201304/part-01/warcs/DOTUK-HISTORICAL-2011-201304-WARCS-PART-00044-601503-000001.warc.gz'])
