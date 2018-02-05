import os
import json
import zlib
import luigi
import luigi.format
import luigi.contrib.hdfs
from luigi.contrib.postgres import PostgresTarget
import StringIO
import datetime
from azure.storage.blob import BlockBlobService
from ukwa.tasks.hadoop.hdfs_tasks import ListAllFilesPutOnHDFS
from ukwa.tasks.common import state_file
from ukwa.tasks.common import logger


def taskdb_target(task_group, task_result):
    return PostgresTarget(
            host='access',
            database='access_task_state',
            user='access',
            password='access',
            table=task_group,
            update_id=task_result
        )


class UploadToAzure(luigi.Task):
    path = luigi.Parameter()
    container = luigi.Parameter(default='ukwebarchive')
    prefix = luigi.Parameter(default='jisc-uk-web-domain-dataset-1996-2013')

    task_namespace = 'azure'

    block_blob_service = BlockBlobService(
        account_name=os.environ.get('AZURE_ACCOUNT_NAME'),
        account_key=os.environ.get('AZURE_ACCOUNT_KEY')
    )

    def full_path(self):
        return "%s/%s" % (self.prefix, self.path.lstrip('/'))

    def run(self):
        # Upload the BLOB:
        source = luigi.contrib.hdfs.HdfsTarget(path=self.path)
        size = source.fs.client.status(source.path)['length']
        with source.fs.client.read(source.path) as inf:
            self.block_blob_service.create_blob_from_stream(self.container, self.full_path(), inf, max_connections=1, validate_content=True, count=size)

        # Check the BLOB looks okay - i.e. the path exists and BLOB is the right size:
        if self.block_blob_service.exists(self.container, self.full_path()):
            props = self.block_blob_service.get_blob_properties(self.container, self.full_path())
            if props.properties.content_length == size:
                # Looks good, so log this task as complete:
                self.output().touch()
                return
            else:
                raise Exception("Uploaded BLOB reported the wrong size! Expected: %s = %i but got %i" %
                                (self.full_path(), size, props.properties.content_length))
        else:
            raise Exception("Could not find uploaded BLOB! Expected: %s" % self.full_path())

    def output(self):
        """If this all works, record success in the DB"""
        return taskdb_target( 'azure_uploads', '%s UPLOADED' % self.path)


class UploadFilesToAzure(luigi.Task):
    part_id = luigi.Parameter()
    path_list = luigi.ListParameter()

    task_namespace = 'azure'

    def requires(self):
        # Attempt to upload each item in this chunk:
        for item in self.path_list:
            yield UploadToAzure(item)

    def output(self):
        slug = "part-%s-%s" % (self.part_id, len(self.path_list))
        return state_file(None, 'hdfs-%s' % slug, 'turing-upload-done.txt')

    def run(self):
        # If all the required tasks worked, record success:
        with self.output().open('w') as f:
            f.write("COMPLETED\t%s" % datetime.date.today())


class UploadFilesToAzureAndRecord(luigi.Task):
    """
    Just copies the result of the parent task into the task DB
    """
    part_id = luigi.Parameter()
    path_list = luigi.ListParameter()

    task_namespace = 'azure'

    def requires(self):
        return UploadFilesToAzure(self.part_id, self.path_list)

    def run(self):
        # Record output in DB too:
        for item in self.path_list:
            tr = UploadToAzure(item).output()
            if not tr.exists():
                tr.touch()

        # All done, so log this task as complete:
        self.output().touch()


    def output(self):
        """If this all works, record success in the DB"""
        return taskdb_target( 'azure_upload_set', '%s UPLOADED' % self.part_id)


class ListFilesToUploadToAzure(luigi.Task):
    """
    Takes the full WARC list and filters UKWA content by folder.

    Fixed date and path as we want to sync up a fixed set of files.

    If you yield all the tasks together from `requires` then it hits the task limit.
    So this dynamically yields the tasks in chunks.
    """
    date = luigi.DateParameter()
    path_match = luigi.Parameter()

    task_namespace = 'azure'

    def requires(self):
        return ListAllFilesPutOnHDFS(self.date)

    def output(self):
        slug = str(self.path_match).replace(r'/', '-').strip('/')
        return state_file(self.date, 'hdfs-%s' % slug, 'turing-upload-todo.txt')

    def run(self):
        file_list = self.input()
        with file_list.open('r') as reader:
            with self.output().open('w') as f:
                for line in reader:
                    item = json.loads(line.strip())
                    if item['filename'].startswith(self.path_match):
                        f.write("%s\n" % item['filename'].rstrip())


class UploadDatasetToAzure(luigi.Task):
    """
    Takes the full WARC list and filters UKWA content by folder.

    Fixed date and path as we want to sync up a fixed set of files.

    If you yield all the tasks together from `requires` then it hits the task limit.
    So this dynamically yields the tasks in chunks.
    """
    date = luigi.DateParameter(default=datetime.datetime.strptime('2018-01-26', '%Y-%m-%d'))
    path_match = luigi.Parameter(default='/ia/1996-2010/')

    task_namespace = 'azure'

    def slug(self):
        return str(self.path_match).replace(r'/', '-').strip('/')

    def requires(self):
        return ListFilesToUploadToAzure(self.date, self.path_match)

    def output(self):
        slug = self.slug()
        return state_file(self.date, 'hdfs-%s' % slug, 'turing-upload-done.txt')

    def run(self):
        file_list = self.input()
        part = 0
        with file_list.open('r') as reader:
            items = []
            for line in reader:
                item = line.strip()
                items.append(item)
                if len(items) >= 100:
                    yield UploadFilesToAzureAndRecord('%s-%i' % (self.slug(), part), items)
                    part = part + 1
                    items = []
            # Catch the last chunk:
            if len(items) > 0:
                yield UploadFilesToAzure('last', items)

        with self.output().open('w') as f:
            f.write("COMPLETED\t%s" % datetime.date.today())



if __name__ == '__main__':
    luigi.run(['UploadDatasetToAzure', '--workers', '40'])
    #luigi.run(['ListFilesToUploadToAzure', '--local-scheduler' , '--path-match' , '/user/root/input/hadoop'])
    #luigi.run(['UploadToAzure', '--path', '/ia/2011-201304/part-01/warcs/DOTUK-HISTORICAL-2011-201304-WARCS-PART-00044-601503-000001.warc.gz'])
