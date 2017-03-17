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
from tasks.common import check_hash, get_large_interval

logger = logging.getLogger('luigi-interface')

LUIGI_STATE_FOLDER = os.environ['LUIGI_STATE_FOLDER']
HDFS_PREFIX = os.environ['HDFS_PREFIX']
WEBHDFS_PREFIX = os.environ['WEBHDFS_PREFIX']

CRAWL_JOB_FOLDER = os.environ.get('local_job_folder','/heritrix/jobs')
CRAWL_OUTPUT_FOLDER = os.environ.get('local_output_folder','/heritrix/output')
WREN_FOLDER =  os.environ.get('local_wren_folder','/heritrix/wren')
SIPS_FOLDER =  os.environ.get('local_sips_folder','/heritrix/sips')


def hash_target(path):
    return luigi.LocalTarget('{}/files/hash/{}'.format(LUIGI_STATE_FOLDER, path.replace('/','_')))


class AwaitUploadRemoteFileToHDFS(luigi.ExternalTask):
    task_namespace = 'file'
    host = luigi.Parameter()
    source_path = luigi.Parameter()
    target_path = luigi.Parameter()

    def output(self):
        t = luigi.contrib.hdfs.HdfsTarget(self.target_path)
        logger.info("Target on HDFS is %s" % t.path)
        return t


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
    resources = {'webhdfs': 1}

    def output(self):
        t = luigi.contrib.hdfs.HdfsTarget(self.target_path)
        logger.info("Output is %s" % t.path)
        return t

    def run(self):
        """
        The local file is self.source_path
        The remote file is self.output().path

        :return: None
        """
        self.uploader(self.host, self.source_path, self.output().path)

    @staticmethod
    def uploader(host, local_path, hdfs_path, username="root"):
        """
        Copy up to HDFS, making it suitably atomic by using a temporary filename during upload.

        Done as a static method to prevent accidental confusion of self.path/self.output().path etc.

        :return: None
        """
        # Set up the HDFS client:
        client = luigi.contrib.hdfs.WebHdfsClient()

        # Create the temporary file name:
        tmp_path = "%s.temp" % hdfs_path

        # Now upload the file, allowing overwrites as this is a temporary file and
        # simultanous updates should not be possible:
        logger.info("Uploading as %s" % tmp_path)

        # n.b. uses CURL to push content via WebHDFS/HttpFS:
        #one = 'curl -L -i -X PUT -T local_file "http://:50075/webhdfs/v1/?op=CREATE..."'
        #two = 'curl -X PUT -L "http://host:port/webhdfs/v1/tmp/myLargeFile.zip?op=CREATE&data=true" --header "Content-Type:application/octet-stream" --header "Transfer-Encoding:chunked" -T "/myLargeFile.zip"'
        rc = luigi.contrib.ssh.RemoteContext(host)
        cmd = ['curl', '-X', 'PUT', '-H', 'Content-Type: application/octet-stream', '-L', '-T', '"%s"' % local_path,
               '"%s%s?op=CREATE&user.name=%s&data=true"' % (WEBHDFS_PREFIX, tmp_path, username) ]
        logger.debug("UPLOADER: %s" % " ".join(cmd))
        rc.check_output(cmd)

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


class MoveToHdfs(luigi.Task):
    task_namespace = 'file'

    host = luigi.Parameter()
    source_path = luigi.Parameter()
    target_path = luigi.Parameter()
    delete_local = luigi.BoolParameter(default=False)
    upload = luigi.BoolParameter(default=False)

    def requires(self):
        if self.upload:
            return UploadRemoteFileToHDFS(self.host, self.source_path, self.target_path)
        else:
            return AwaitUploadRemoteFileToHDFS(self.host, self.source_path, self.target_path)

    def output(self):
        return hash_target("%s.transferred" % self.target_path)

    def calculate_remote_hash(self):
        logger.debug("Remote file %s to hash" % self.source_path)

        t = luigi.contrib.ssh.RemoteTarget(path=self.source_path,host=self.host)#, **SSH_KWARGS)
        with t.open('r') as reader:
            file_hash = hashlib.sha512(reader.read()).hexdigest()

        # test hash is sane:
        check_hash(self.source_path, file_hash)

        # And return it:
        return file_hash

    def calculate_hdfs_hash(self):
        logger.debug("HDFS file %s to hash" % (self.target_path))

        # get hash for local or hdfs file
        client = luigi.contrib.hdfs.WebHdfsClient()
        # Having to side-step the first client as it seems to be buggy/use an old API - note also confused put()
        with client.client.read(str(self.target_path)) as reader:
            file_hash = hashlib.sha512(reader.read()).hexdigest()

        # test hash is sane
        check_hash(self.target_path, file_hash)

        # And return it:
        return file_hash

    def run(self):
        # Calculate SHA512 of remote file:
        remote_hash = self.calculate_remote_hash()
        logger.info("Got remote hash %s" % remote_hash)

        # Re-download and get the hash from HDFS:
        hdfs_hash = self.calculate_hdfs_hash()
        logger.info("Got HDFS hash %s" % hdfs_hash)

        # Compare them:
        if remote_hash != hdfs_hash:
            raise Exception("Remote & HDFS hashes do not match for %s > %s" % (self.source_path, self.target_path))

        # Otherwise, move to hdfs was good, so delete:
        if self.delete_local:
            logger.error("DELETE NOT IMPLEMENTED!")
            # os.remove(str(self.path))

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
        return luigi.contrib.ssh.RemoteTarget(path=self.source_path, host=self.host)

    # Specify the target folder:
    def output(self):
        # FIXME Parse job and launch ID and generate target_path
        target_path = self.path
        return luigi.contrib.ssh.RemoteTarget(path=target_path, host=self.host)

    def get_date_prefix(path):
        timestamp = re.findall(r"\D(\d{14}|\d{17})\D", os.path.basename(path))
        if len(timestamp) > 0:
            return "%s-%s" % (timestamp[0][0:4], timestamp[0][4:6])
        else:
            return "none"

    # When run, just move the file:
    # We must take care to make this atomic as move can be copy if the systems are presented as different drives.
    def run(self):
        rf = luigi.contrib.ssh.RemoteFileSystem(self.host)
        rf.move(self.input().path, self.output().path)


class ScanForFilesToMove(luigi.WrapperTask):
    """
    Look for files ready to be moved up to HDFS.  First moves any closed WREN WARCS, then attempts to move the
    important crawl files, mostly WARCs and logs.
    """
    task_namespace = 'move'
    host = luigi.Parameter()
    remote_prefix = luigi.Parameter(default="")
    delete_local = luigi.BoolParameter(default=False)
    date_interval = luigi.DateIntervalParameter(default=get_large_interval())
    upload = luigi.BoolParameter(default=False)

    def requires(self):
        """
        This yields all the moves to be done.

        :return:
        """
        # Set up base paths:
        local_job_folder = "%s%s" %( self.remote_prefix, CRAWL_JOB_FOLDER)
        local_output_folder = "%s%s" %( self.remote_prefix, CRAWL_OUTPUT_FOLDER )
        local_wren_folder = "%s%s" %( self.remote_prefix, WREN_FOLDER)
        local_sip_folder = "%s%s" %( self.remote_prefix, SIPS_FOLDER)
        rf = luigi.contrib.ssh.RemoteFileSystem(self.host)

        # Look in /heritrix/output/wren files and move closed WARCs to the /warcs/ folder:
        for wren_source in self.listdir_in_date_range(rf, local_wren_folder, "*.warc.gz"):
            logger.debug("WREN MOVE: %s" % wren_source)
            yield MoveRemoteWrenWarcFile(self.host, wren_source)
            
        # Look in warcs and viral for WARCs e.g in /heritrix/output/{warcs|viral}/**/*.warc.gz
        for out_type in ['warcs', 'viral']:
            for item in self.listdir_in_date_range(rf, "%s/%s" % (local_output_folder, out_type), "*.warc.gz"):
                logger.debug("WARC: %s" % item)
                yield self.request_move(item)

        # And look for /heritrix/output/logs/**/*.log*:
        for log_item in self.listdir_in_date_range(rf, "%s/logs/" % local_output_folder, "*.log*"):
            logger.debug("WARC: %s" % log_item)
            if os.path.splitext(log_item)[1] == '.lck':
                continue
            elif os.path.splitext(log_item)[1] == '.log':
                # Only move files with the '.log' suffix if this job is no-longer running (based on .lck file):
                if not rf.exists("%s.lck" % log_item):
                    yield self.request_move(log_item)
                else:
                    logger.info("Can't move locked log: %s" % log_item)
            else:
                yield self.request_move(log_item)

    def request_move(self, item):
        logger.info("Requesting Move to HDFS for:%s" % item)
        return MoveToHdfs(self.host, item, self.hdfs_path(item), self.delete_local, self.upload)

    def hdfs_path(self, path):
        # Chop out any local prefix:
        hdfs_path = path[len(self.remote_prefix):]
        # Prefix this path with the HDFS root folder, stripping any leading '/' so the path is considered relative
        if hdfs_path:
            hdfs_path = os.path.join(HDFS_PREFIX, hdfs_path.lstrip("/"))
        return hdfs_path

    def listdir_in_date_range(self, rf, path, match):
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

        # Construct an appropriate `find` command:
        cmd = ["find", "-L", path, "-type", "f", "-newermt",
               self.date_interval.date_a.strftime('"%Y-%m-%d"'), "!", "-newermt",
               self.date_interval.date_b.strftime('"%Y-%m-%d"'), "-name", '"%s"' % match]
        #logger.debug(": %s" % " ".join(cmd))

        listing = rf.remote_context.check_output(cmd).splitlines()
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


class ScanForSIPsToMove(luigi.WrapperTask):
    """
    Look for files ready to be moved up to HDFS.  First moves any closed WREN WARCS, then attempts to move the
    important crawl files, mostly WARCs and logs.
    """
    task_namespace = 'move'
    host = luigi.Parameter()
    remote_prefix = luigi.Parameter(default="")
    delete_local = luigi.BoolParameter(default=False)
    upload = luigi.BoolParameter(default=False)

    def requires(self):
        """
        This yields all the moves to be done.

        :return:
        """
        # Set up base paths:
        local_sip_folder = "%s%s" % (self.remote_prefix, SIPS_FOLDER)
        rf = luigi.contrib.ssh.RemoteFileSystem(self.host)

        # Look in warcs and viral for WARCs e.g in /heritrix/output/{warcs|viral}/**/*.warc.gz
        tasks = []
        for item in rf.listdir("%s/*/*.tar.gz" % local_sip_folder):
            tasks.append(self.request_move(item))

        # Yield all together:
        yield tasks

    def request_move(self, item):
        logger.info("Requesting Move to HDFS for:%s" % item)
        return MoveToHdfs(self.host, item, self.hdfs_path(item), self.delete_local, self.upload)

    def hdfs_path(self, path):
        # Chop out any local prefix:
        hdfs_path = path[len(self.remote_prefix):]
        # Prefix this path with the HDFS root folder, stripping any leading '/' so the path is considered relative
        if hdfs_path:
            hdfs_path = os.path.join(HDFS_PREFIX, hdfs_path.lstrip("/"))
        return hdfs_path


if __name__ == '__main__':
    luigi.run(['move.ScanForSIPsToMove', '--workers', '5', '--host' , 'crawler03', '--upload'])
    #luigi.run(['move.ScanForFilesToMove', '--date-interval', '2017-02-11-2017-02-12', '--host' , 'localhost',
    #           '--remote-prefix', '/Users/andy/Documents/workspace/pulse/testing' , '--local-scheduler'])
    #luigi.run(['file.ForceUploadFileToHDFS', '--path', '/Users/andy/Documents/workspace/pulse/testing/output/logs/daily/20161029192642/progress-statistics.log'])
#    luigi.run(['file.ScanForFiles', '--date-interval', '2016-10-26-2016-10-30'])  # , '--local-scheduler'])
#    luigi.run(['file.MoveToHdfs', '--path', '/Users/andy/Documents/workspace/pulse/python-shepherd/MANIFEST.in'])  # , '--local-scheduler'])

