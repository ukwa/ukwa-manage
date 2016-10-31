import os
import glob
import luigi
import string
import datetime
import hashlib
import threading
import subprocess
import luigi.contrib.hdfs
from common import *
from crawl_job_tasks import CheckJobStopped


HDFS_PREFIX = "/1_data"


def get_hdfs_path(path):
    return "%s%s" % (HDFS_PREFIX, path)


class UploadFileToHDFS(luigi.Task):
    task_namespace = 'file'
    path = luigi.Parameter()

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget(get_hdfs_path(self.path))

    def run(self):
        # Copy up to HDFS
        client = luigi.contrib.hdfs.get_autoconfig_client(threading.local())
        logger.info("Client:  %s" % client)
        client.upload(self.output().path, self.path)


class ForceUploadFileToHDFS(luigi.Task):
    """
    Variant of UploadFileToHDFS that allows overwriting the HDFS file.

    Implemented as a separate task to minimise likelihood of overwrite being enabled accidentally.
    """
    task_namespace = 'file'
    path = luigi.Parameter()

    # For this to re-run, even if local state marks it as 'complete'
    def complete(self):
        return False

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget(get_hdfs_path(self.path))

    def run(self):
        # Copy up to HDFS
        client = luigi.contrib.hdfs.get_autoconfig_client(threading.local())
        logger.info("HDFS hash, pre:  %s" % client.client.checksum(self.output().path))
        client.upload(self.output().path, self.path, overwrite=True)
        logger.info("HDFS hash, post:  %s" % client.client.checksum(self.output().path))


class CalculateLocalHash(luigi.Task):
    task_namespace = 'file'
    path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget('{}/file/{}.local.sha512'.format(state().state_folder, self.path))

    def run(self):
        logger.debug("file %s to hash" % (self.path))

        t = luigi.LocalTarget(self.path)
        with t.open('r') as reader:
            file_hash = hashlib.sha512(reader.read()).hexdigest()

        # test hash
        CalculateLocalHash.check_hash(self.path, file_hash)

        with self.output().open('w') as f:
            f.write(file_hash)

    @staticmethod
    def check_hash(path, file_hash):
        logger.debug("Checking file %s hash %s" % (path, file_hash))
        if len(file_hash) != 128:
            raise Exception("%s hash not 128 character length [%s]" % (path, len(file_hash)))
        if not all(c in string.hexdigits for c in file_hash):
            raise Exception("%s hash not all hex [%s]" % (path, file_hash))


class CalculateHdfsHash(luigi.Task):
    task_namespace = 'file'
    path = luigi.Parameter()

    def requires(self):
        return UploadFileToHDFS(self.path)

    def output(self):
        return luigi.LocalTarget('{}/file/{}.hdfs.sha512'.format(state().state_folder, self.path))

    def run(self):
        logger.debug("file %s to hash" % (self.path))

        # get hash for local or hdfs file
        t = self.input()
        client = luigi.contrib.hdfs.get_autoconfig_client(threading.local())
        logger.info("t2 %s " % t.fs)
        # Having to side-step the first client as it seems to be buggy/use an old API - note also confused put()
        with client.client.read(str(t.path)) as reader:
            file_hash = hashlib.sha512(reader.read()).hexdigest()

        # test hash
        CalculateLocalHash.check_hash(self.path, file_hash)

        with self.output().open('w') as f:
            f.write(file_hash)


class MoveToHdfs(luigi.Task):
    task_namespace = 'file'
    path = luigi.Parameter()
    delete_local = luigi.BoolParameter(default=False)
    if_stopped = luigi.BoolParameter(default=False)

    def requires(self):
        if self.if_stopped:
            return
        return [ CalculateLocalHash(self.path),  CalculateHdfsHash(self.path) ]

    def output(self):
        return luigi.LocalTarget('{}/file/{}.transferred'.format(state().state_folder, self.path))

    def run(self):
        # Read in sha512
        with self.input()[0].open('r') as f:
            local_hash = f.readline()
        logger.info("Got local hash %s" % local_hash)
        # Re-download and get the hash
        with self.input()[1].open('r') as f:
            hdfs_hash = f.readline()
        logger.info("Got HDFS hash %s" % hdfs_hash)

        if local_hash != hdfs_hash:
            raise Exception("Local & HDFS hashes do not match for %s" % self.path)

        # Otherwise, move to hdfs was good, so delete:
        if self.delete_local:
            os.remove(str(self.path))
        # and write out success
        with self.output().open('w') as f:
            f.write(hdfs_hash)


class MoveToHdfsIfStopped(luigi.Task):
    task_namespace = 'file'
    job = luigi.EnumParameter(enum=Jobs)
    launch_id = luigi.Parameter()
    path = luigi.Parameter()
    delete_local = luigi.BoolParameter(default=False)

    # Require that the job is stopped:
    def requires(self):
        return CheckJobStopped(self.job, self.launch_id)

    # Use the output of the underlying MoveToHdfs call:
    def output(self):
        return MoveToHdfs(self.path, self.delete_local).output()

    # Call the MoveToHdfs task as a dynamic dependency:
    def run(self):
        yield MoveToHdfs(self.path, self.delete_local)


class ScanJobLaunchFiles(luigi.WrapperTask):
    task_namespace = 'file'
    job = luigi.EnumParameter(enum=Jobs)
    launch_id = luigi.Parameter()
    delete_local = luigi.BoolParameter(default=False)

    def requires(self):
        # Look in warcs and viral for WARCs
        for out_type in ['warcs', 'viral']:
            for item in glob.glob("%s/output/%s/%s/%s/*.warc.gz" % (h3().local_root_folder, out_type, self.job.name, self.launch_id)):
                yield MoveToHdfs(item, self.delete_local)
        # Look in wren too:
        for wren_item in glob.glob("%s/output/wren/*-%s-%s-*.warc.gz" % (h3().local_root_folder, self.job.name, self.launch_id)):
            yield MoveToHdfs(wren_item, self.delete_local)
        # And look for logs:
        for log_item in glob.glob("%s/output/logs/%s/%s/*.log*" % (h3().local_root_folder, self.job.name, self.launch_id)):
            if os.path.splitext(log_item)[1] == '.lck':
                continue
            elif os.path.splitext(log_item)[1] == '.log':
                yield MoveToHdfsIfStopped(self.job, self.launch_id, log_item, self.delete_local)
            else:
                yield MoveToHdfs(log_item, self.delete_local)


class ScanForFiles(luigi.WrapperTask):
    """
    This task scans the output folder for jobs and instances of those jobs, looking for crawled content to process.
    """
    task_namespace = 'file'
    date_interval = luigi.DateIntervalParameter(
        default=[datetime.date.today() - datetime.timedelta(days=1), datetime.date.today()])
    delete_local = luigi.BoolParameter(default=False)

    def requires(self):
        # Look for jobs that need to be processed:
        for date in self.date_interval:
            for job_item in glob.glob("%s/*" % h3().local_job_folder):
                job = Jobs[os.path.basename(job_item)]
                if os.path.isdir(job_item):
                    launch_glob = "%s/%s*" % (job_item, date.strftime('%Y%m%d'))
                    # self.set_status_message("Looking for job launch folders matching %s" % launch_glob)
                    for launch_item in glob.glob(launch_glob):
                        if os.path.isdir(launch_item):
                            launch = os.path.basename(launch_item)
                            # TODO Limit total number of processes?
                            yield ScanJobLaunchFiles(job, launch)


if __name__ == '__main__':
    luigi.run(['file.ScanForFiles'])
    #luigi.run(['file.ForceUploadFileToHDFS', '--path', '/Users/andy/Documents/workspace/pulse/testing/output/logs/daily/20161029192642/progress-statistics.log'])
#    luigi.run(['file.ScanForFiles', '--date-interval', '2016-10-26-2016-10-30'])  # , '--local-scheduler'])
#    luigi.run(['file.MoveToHdfs', '--path', '/Users/andy/Documents/workspace/pulse/python-shepherd/MANIFEST.in'])  # , '--local-scheduler'])

