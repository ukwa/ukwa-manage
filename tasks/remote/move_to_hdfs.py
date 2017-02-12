import os
import re
import time
import luigi
import hashlib
import logging
import datetime
import threading
import luigi.contrib.hdfs
import luigi.contrib.ssh
from tasks.common import hash_target, check_hash

logger = logging.getLogger('luigi-interface')

LUIGI_STATE_FOLDER = os.environ['LUIGI_STATE_FOLDER']
HDFS_PREFIX = os.environ['HDFS_PREFIX']

CRAWL_JOB_FOLDER = os.environ.get('local_job_folder','/heritrix/jobs')
CRAWL_OUTPUT_FOLDER = os.environ.get('local_output_folder','/heritrix/output')
WREN_FOLDER =  os.environ.get('local_wren_folder','/heritrix/wren')


class UploadRemoteFileToHDFS(luigi.Task):
    """
    This copies up to HDFS but uses a temporary filename (via a suffix) to avoid downstream tasks
    thinking the work is already done.

    Uses curl remotely to push content up via the HttpFS API.
    """
    task_namespace = 'file'
    host = luigi.Parameter()
    source_path = luigi.Parameter()
    target_path = luigi.Parameter()
    resources = {'hdfs': 1}

    def output(self):
        t = luigi.contrib.hdfs.HdfsTarget(self.target_path)
        logger.info("Output is %s" % t.path)
        return t

    def run(self):
        """
        The local file is self.path
        The remote file is self.output().path

        :return: None
        """
        self.uploader(self.source_path, self.output().path)

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

        one = 'curl -L -i -X PUT -T local_file "http://:50075/webhdfs/v1/?op=CREATE..."'
        two = 'curl -X PUT -L "http://host:port/webhdfs/v1/tmp/myLargeFile.zip?op=CREATE&data=true" --header "Content-Type:application/octet-stream" --header "Transfer-Encoding:chunked" -T "/myLargeFile.zip"'
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


class CalculateHdfsHash(luigi.Task):
    task_namespace = 'file'
    host = luigi.Parameter()
    source_path = luigi.Parameter()
    target_path = luigi.Parameter()
    resources = { 'hdfs': 1 }

    def requires(self):
        return UploadRemoteFileToHDFS(self.path)

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

        # test hash is sane
        check_hash(self.path, file_hash)

        with self.output().open('w') as f:
            f.write(file_hash)


class CalculateRemoteHash(luigi.Task):
    task_namespace = 'output'
    host = luigi.Parameter()
    path = luigi.Parameter()

    def output(self):
        return self.hash_target("%s.local.sha512" % self.path)

    def run(self):
        logger.debug("file %s to hash" % self.path)

        t = luigi.contrib.ssh.RemoteTarget(self.path,self.host)#, **SSH_KWARGS)
        with t.open('r') as reader:
            file_hash = hashlib.sha512(reader.read()).hexdigest()

        # test hash is sane
        check_hash(self.path, file_hash)

        with self.output().open('w') as f:
            f.write(file_hash)


class MoveToHdfs(luigi.Task):
    task_namespace = 'file'
    host = luigi.Parameter()
    source_path = luigi.Parameter()
    target_path = luigi.Parameter()
    delete_local = luigi.BoolParameter(default=False)

    def requires(self):
        return [ CalculateRemoteHash(self.job, self.launch_id, self.path),
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


class MoveRemoteWrenWarcFile(luigi.Task):
    """
    Moves closed 'WREN' WARCs to the appropriate WARC folder.
    """
    task_namespace = 'file'
    host = luigi.Parameter()
    path = luigi.Parameter()

    # Requires the source path to be present and closed:
    def requires(self):
        return luigi.contrib.ssh.RemoteTarget(self.host, self.source_path)

    # Specify the target folder:
    def output(self):
        # FIXME Parse job and launch ID and generate target_path
        target_path = self.path
        return luigi.contrib.ssh.RemoteTarget(self.host, target_path)

    def get_date_prefix(path):
        timestamp = re.findall(r"\D(\d{14}|\d{17})\D", os.path.basename(path))
        if len(timestamp) > 0:
            return "%s-%s" % (timestamp[0][0:4], timestamp[0][4:6])
        else:
            return "none"

    # When run, just move the file:
    def run(self):
        rf = luigi.contrib.ssh.RemoteFileSystem(self.host)
        rf.move(self.input().path, self.output().path)


class ScanForFilesToMove(luigi.WrapperTask):
    """
    Look for files ready to be moved up to HDFS.  First moves any closed WREN WARCS, then attempts to move the
    important crawl files, mostly WARCs and logs.
    """
    task_namespace = 'move-to-hdfs'
    host = luigi.Parameter()
    local_prefix = luigi.Parameter(default="/zfspool")
    delete_local = luigi.BoolParameter(default=False)
    date_interval = luigi.DateIntervalParameter(
        default=[datetime.date.today() - datetime.timedelta(days=1), datetime.date.today()])

    def requires(self):
        """
        This yields all the moves to be done.

        :return:
        """
        # Set up base paths:
        local_job_folder = "%s%s" %( self.local_prefix, CRAWL_JOB_FOLDER)
        local_output_folder = "%s%s" %( self.local_prefix, CRAWL_OUTPUT_FOLDER )
        local_wren_folder = "%s%s" %( self.local_prefix, WREN_FOLDER)
        rf = luigi.contrib.ssh.RemoteFileSystem(self.host)

        # Look in /heritrix/output/wren files and move closed WARCs to the /warcs/ folder:
        for wren_source in self.listdir_in_date_range(rf, local_wren_folder, "*.warc.gz"):
            yield MoveRemoteWrenWarcFile(self.host, wren_source)
            
        # Look in warcs and viral for WARCs e.g in /heritrix/output/{warcs|viral}/**/*.warc.gz
        for out_type in ['warcs', 'viral']:
            glob_path = "%s/%s/%s/%s/*.warc.gz" % (local_output_folder, out_type, self.job.name, self.launch_id)
            for item in self.listdir_in_date_range(rf, "%s/%s" % (local_output_folder, out_type), "*.warc.gz"):
                self.request_move(item)

        # And look for /heritrix/output/logs/**/*.log*:
        for log_item in self.listdir_in_date_range(rf, "%s/logs/" % local_output_folder, "*.log*"):
            if os.path.splitext(log_item)[1] == '.lck':
                continue
            elif os.path.splitext(log_item)[1] == '.log':
                # Only move files with the '.log' suffix if this job is no-longer running (based on .lck file):
                if not rf.exists("%s.lck" % log_item):
                    self.request_move(item)
                else:
                    logger.info("Can't move locked log: %s" % log_item)
            else:
                self.request_move(item)

    def request_move(self, item):
        logger.info("Requesting Move to HDFS for:%s" % item)
        yield MoveToHdfs(item, self.hdfs_path(item), self.delete_local)

    def hdfs_path(self, path):
        # Chop out any local prefix:
        hdfs_path = path[len(self.local_prefix):]
        # Prefix this path with the HDFS root folder, stripping any leading '/' so the path is considered relative
        if hdfs_path:
            hdfs_path = os.path.join(HDFS_PREFIX, hdfs_path.lstrip("/"))
        return hdfs_path

    @staticmethod
    def listdir_in_date_range(rf, path, match):
        """
        Based on RemoteFileSystem.listdir but non-recursive.

        # find . -type f -newermt "2010-01-01" ! -newermt "2010-06-01"

        :param rf:
        :param path:
        :return:
        """
        logger.info("Looking in %s %s" % (path, match))
        while path.endswith('/'):
            path = path[:-1]

        path = path or '.'

        # If the parent folder does not exists, there are no matches:
        if not rf.exists(path):
            return []

        listing = rf.remote_context.check_output(["find", "-L", path, "-type", "f" ,"-name", '"%s"' % match]).splitlines()
        return [v.decode('utf-8') for v in listing]


    @staticmethod
    def remote_ls(parent, glob, rf):
        """
        Based on RemoteFileSystem.listdir but non-recursive.

        :param parent:
        :param glob:
        :param rf:
        :return:
        """
        while parent.endswith('/'):
            parent = parent[:-1]

        parent = parent or '.'

        # If the parent folder does not exists, there are no matches:
        if not rf.exists(parent):
            return []

        listing = rf.remote_context.check_output(["find", "-L", parent, '-maxdepth', '1', '-name', '"%s"' % glob]).splitlines()
        return [v.decode('utf-8') for v in listing]


if __name__ == '__main__':
    luigi.run(['scan.ScanForFilesToMove', '--date-interval', '2016-11-01-2016-11-10'])
    #luigi.run(['file.ForceUploadFileToHDFS', '--path', '/Users/andy/Documents/workspace/pulse/testing/output/logs/daily/20161029192642/progress-statistics.log'])
#    luigi.run(['file.ScanForFiles', '--date-interval', '2016-10-26-2016-10-30'])  # , '--local-scheduler'])
#    luigi.run(['file.MoveToHdfs', '--path', '/Users/andy/Documents/workspace/pulse/python-shepherd/MANIFEST.in'])  # , '--local-scheduler'])

