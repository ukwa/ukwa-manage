import os
import re
import glob
import time
import luigi
import string
import datetime
import hashlib
import threading
import subprocess
import luigi.contrib.hdfs
import luigi.contrib.hadoop_jar
from common import *
from crawl_job_tasks import CheckJobStopped


def get_hdfs_path(path):
    # Prefix the original path with the HDFS root folder, stripping any leading '/' so the path is considered relative
    return os.path.join(h3().hdfs_root_folder, path.lstrip(os.path.sep))


def get_hdfs_target(path):
    return luigi.contrib.hdfs.HdfsTarget(get_hdfs_path(path))


def get_date_prefix(path):
    timestamp = re.findall(r"\D(\d{14}|\d{17})\D", os.path.basename(path))
    if len(timestamp) > 0:
        return "%s-%s" % (timestamp[0][0:4], timestamp[0][4:6])
    else:
        return "none"


class UploadFileToHDFS(luigi.Task):
    """
    This copies up to HDFS but uses a temporary filename (via a suffix) to avoid downstream tasks
    thinking the work is already done.
    """
    task_namespace = 'file'
    path = luigi.Parameter()
    resources = {'hdfs': 1}

    def output(self):
        t = get_hdfs_target(self.path)
        logger.info("Output is %s" % t.path)
        return t

    def run(self):
        """
        The local file is self.path
        The remote file is self.output().path

        :return: None
        """
        self.uploader(self.path, self.output().path)

    @staticmethod
    def uploader(local_path, hdfs_path):
        """
        Copy up to HDFS, making it suitably atomic by using a temporary filename during upload.

        Done as a static method to prevent accidental confusion of self.path/self.output().path etc.

        :return: None
        """
        # Set up the HDFS client:
        client = luigi.contrib.hdfs.get_autoconfig_client(threading.local())

        # Create the temporary file name:
        tmp_path = "%s.temp" % hdfs_path

        # Now upload the file, allowing overwrites as this is a temporary file and
        # simultanous updates should not be possible:
        logger.info("Uploading as %s" % tmp_path)
        with open(local_path, 'r') as f:
            client.client.write(data=f, hdfs_path=tmp_path, overwrite=True)

        # Check if the destination file exists and raise an exception if so:
        if client.exists(hdfs_path):
            raise Exception("Path %s already exists! This should never happen!" % hdfs_path)

        # Move the uploaded file into the right place:
        client.client.rename(tmp_path, hdfs_path)

        # Give the namenode a moment to catch-up with itself and then check it's there:
        # FIXME I suspect this is only needed for our ancient HDFS
        time.sleep(2)
        status = client.client.status(hdfs_path)

        # Log successful upload:
        logger.info("Upload completed for %s" % hdfs_path)



class ForceUploadFileToHDFS(luigi.Task):
    """
    Variant of UploadFileToHDFS that allows direct overwriting the HDFS file (no '.temp').

    Implemented as a separate task to minimise likelihood of overwrite being enabled accidentally.

    Not part of the main task chain - intended to be launched manually only.
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
        with open(self.path, 'r') as f:
            client.client.write(data=f, hdfs_path=self.output().path, overwrite=True)
        logger.info("HDFS hash, post:  %s" % client.client.checksum(self.output().path))


class CloseOpenWarcFile(luigi.Task):
    """
    This task can close files that have been left .open, but it is not safe to tie this in usually as WARCs from
    warcprox do not get closed when the crawler runs a checkpoint.
    """
    task_namespace = 'file'
    job = luigi.EnumParameter(enum=Jobs)
    launch_id = luigi.Parameter()
    path = luigi.Parameter()

    # Require that the job is stopped:
    def requires(self):
        return CheckJobStopped(self.job, self.launch_id)

    def output(self):
        return luigi.LocalTarget(self.path)

    def run(self):
        open_path = "%s.open" % self.path
        if os.path.isfile(open_path) and not os.path.isfile(self.path):
            logger.info("Found an open file that needs closing: %s " % open_path)
            os.rename(open_path, self.path)


class ClosedWarcFile(luigi.ExternalTask):
    """
    An external process is responsible to closing open WARC files, so we declare it here.
    """
    task_namespace = 'file'
    job = luigi.EnumParameter(enum=Jobs)
    launch_id = luigi.Parameter()
    path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.path)


class CalculateLocalHash(luigi.Task):
    task_namespace = 'file'
    job = luigi.EnumParameter(enum=Jobs)
    launch_id = luigi.Parameter()
    path = luigi.Parameter()
    force_close = luigi.BoolParameter(default=False)

    # If instructed, force the closure of any open WARC files. Not safe to use this be default (see above)
    def requires(self):
        if self.force_close:
            return CloseOpenWarcFile(self.job, self.launch_id, self.path)
        else:
            return ClosedWarcFile(self.job, self.launch_id, self.path)

    def output(self):
        return hash_target(self.job, self.launch_id, "%s.local.sha512" % self.path)

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
    job = luigi.EnumParameter(enum=Jobs)
    launch_id = luigi.Parameter()
    path = luigi.Parameter()
    resources = { 'hdfs': 1 }

    def requires(self):
        return UploadFileToHDFS(self.path)

    def output(self):
        return hash_target(self.job, self.launch_id, "%s.hdfs.sha512" % self.path)

    def run(self):
        logger.debug("file %s to hash" % (self.path))

        # get hash for local or hdfs file
        t = self.input()
        client = luigi.contrib.hdfs.get_autoconfig_client(threading.local())
        # Having to side-step the first client as it seems to be buggy/use an old API - note also confused put()
        with client.client.read(str(t.path)) as reader:
            file_hash = hashlib.sha512(reader.read()).hexdigest()

        # test hash
        CalculateLocalHash.check_hash(self.path, file_hash)

        with self.output().open('w') as f:
            f.write(file_hash)


class MoveToHdfs(luigi.Task):
    task_namespace = 'file'
    job = luigi.EnumParameter(enum=Jobs)
    launch_id = luigi.Parameter()
    path = luigi.Parameter()
    delete_local = luigi.BoolParameter(default=False)

    def requires(self):
        return [ CalculateLocalHash(self.job, self.launch_id, self.path),
                 CalculateHdfsHash(self.job, self.launch_id, self.path) ]

    def output(self):
        return hash_target(self.job, self.launch_id, "%s.transferred" % self.path)

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
        return MoveToHdfs(self.job, self.launch_id, self.path, self.delete_local).output()

    # Call the MoveToHdfs task as a dynamic dependency:
    def run(self):
        yield MoveToHdfs(self.job, self.launch_id, self.path, self.delete_local)


class MoveToWarcsFolder(luigi.Task):
    """
    This can used to move a WARC that's outside the
    """
    task_namespace = 'file'
    job = luigi.EnumParameter(enum=Jobs)
    launch_id = luigi.Parameter()
    path = luigi.Parameter()

    # Requires the source path to be present and closed:
    def requires(self):
        return ClosedWarcFile(self.job, self.launch_id, self.path)

    # Specify the target folder:
    def output(self):
        return luigi.LocalTarget("%s/output/warcs/%s/%s/%s" % (h3().local_root_folder, self.job.name, self.launch_id,
                                                               os.path.basename(self.path)))

    # When run, just move the file:
    def run(self):
        os.rename(self.path, self.output().path)


class MoveFilesForLaunch(luigi.Task):
    """
    Move all the files associated with one launch
    """
    task_namespace = 'file'
    job = luigi.EnumParameter(enum=Jobs)
    launch_id = luigi.Parameter()
    delete_local = luigi.BoolParameter(default=False)

    def output(self):
        return otarget(self.job, self.launch_id, "all-moved")

    def run(self):
        logger.info("Looking in %s %s" % ( self.job, self.launch_id))
        # Look in /heritrix/output/wren files and move them to the /warcs/ folder:
        for wren_item in glob.glob("%s/*-%s-%s-*.warc.gz" % (h3().local_wren_folder,self. job.name, self.launch_id)):
            yield MoveToWarcsFolder(self.job, self.launch_id, wren_item)
        # Look in warcs and viral for WARCs e.g in /heritrix/output/{warcs|viral}/{job.name}/{launch_id}
        for out_type in ['warcs', 'viral']:
            glob_path = "%s/output/%s/%s/%s/*.warc.gz" % (h3().local_root_folder, out_type, self.job.name, self.launch_id)
            logger.info("GLOB:%s" % glob_path)
            for item in glob.glob("%s/output/%s/%s/%s/*.warc.gz" % (h3().local_root_folder, out_type, self.job.name, self.launch_id)):
                logger.info("ITEM:%s" % item)
                yield MoveToHdfs(self.job, self.launch_id, item, self.delete_local)
        # And look for /heritrix/output/logs:
        for log_item in glob.glob("%s/output/logs/%s/%s/*.log*" % (h3().local_root_folder, self.job.name, self.launch_id)):
            if os.path.splitext(log_item)[1] == '.lck':
                continue
            elif os.path.splitext(log_item)[1] == '.log':
                # Only move files with the '.log' suffix if this job is no-longer running:
                logger.info("Using MoveToHdfsIfStopped for %s" % log_item)
                yield MoveToHdfsIfStopped(self.job, self.launch_id, log_item, self.delete_local)
            else:
                yield MoveToHdfs(self.job, self.launch_id, log_item, self.delete_local)

        # and write out success
        with self.output().open('w') as f:
            f.write("MOVED")


class ScanForFilesToMove(ScanForLaunches):
    """
    This scans for files associated with a particular launch of a given job and starts MoveToHdfs for each,
    """
    delete_local = luigi.BoolParameter(default=False)

    task_namespace = 'scan'
    scan_name = 'move-to-hdfs'

    def scan_job_launch(self, job, launch):
        logger.info("Looking at moving files for %s %s" %(job, launch))
        yield MoveFilesForLaunch(job, launch, self.delete_local)


if __name__ == '__main__':
    luigi.run(['scan.ScanForFilesToMove', '--date-interval', '2016-11-01-2016-11-10'])
    #luigi.run(['file.ForceUploadFileToHDFS', '--path', '/Users/andy/Documents/workspace/pulse/testing/output/logs/daily/20161029192642/progress-statistics.log'])
#    luigi.run(['file.ScanForFiles', '--date-interval', '2016-10-26-2016-10-30'])  # , '--local-scheduler'])
#    luigi.run(['file.MoveToHdfs', '--path', '/Users/andy/Documents/workspace/pulse/python-shepherd/MANIFEST.in'])  # , '--local-scheduler'])

