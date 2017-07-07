import os
import json
import luigi
import luigi.contrib.hdfs
import datetime
from azure.storage.blob import BlockBlobService
from hasher import ListAllFilesOnHDFS
from shepherd.tasks.common import state_file
from shepherd.tasks.common import logger


class UploadToAzure(luigi.Task):
    path = luigi.Parameter()
    container = luigi.Parameter(default='BL Data')

    block_blob_service = BlockBlobService(
        account_name=os.environ.get('AZURE_ACCOUNT_NAME'),
        account_key=os.environ.get('AZURE_ACCOUNT_KEY'))

    def complete(self):
        source = luigi.contrib.hdfs.HdfsTarget(path=self.path)
        status = source.fs.status(source.path)
        # Check the path exists and is the right size:
        if self.block_blob_service.exists(self.container, self.path):
            props = self.block_blob_service.get_blob_properties(self.container, self.path)
            if props.properties.content_length == status['length']:
                return True
        # Wrong...
        return False

    def run(self):
        source = luigi.contrib.hdfs.HdfsTarget(path=self.path)
        with source.open() as inf:
            self.block_blob_service.create_blob_from_stream(self.container, self.path, inf)


class ListDuplicateWebArchiveFilesOnHDFS(luigi.Task):
    """
    Takes the full WARC list and filters UKWA content by folder:
    """
    date = luigi.DateParameter(default=datetime.date.today())
    collection = luigi.Parameter()
    path_match = luigi.Parameter()

    def requires(self):
        return ListAllFilesOnHDFS(self.date)

    def output(self):
        return state_file(self.date, 'hdfs', 'turing-%s-uploaded-file-list.jsonl' % self.collection)

    def run(self):
        filenames = {}
        for line in self.input().open('r'):
            item = json.loads(line.strip())
            # Archive file names:
            basename = os.path.basename(item['filename'])
            if basename not in filenames:
                filenames[basename] = [item['filename']]
            else:
                filenames[basename].append(item['filename'])

        # And emit duplicates:
        unduplicated = 0
        with self.output().open('w') as f:
            for basename in filenames:
                if len(filenames[basename]) > 1:
                    f.write("%s\t%i\t%s\n" % (basename, len(filenames[basename]), json.dumps(filenames[basename])))
                else:
                    unduplicated += 1
        logger.info("Of %i WARC filenames, %i are stored in a single HDFS location." % (len(filenames), unduplicated))


if __name__ == '__main__':
    #luigi.run(['ListUKWAWebArchiveFilesOnHDFS', '--local-scheduler'])
    luigi.run(['UploadToAzure', '--local-scheduler', '--path', '/ia/md5.txt'])
