import os
import re
import csv
import enum
import json
import gzip
import shutil
import logging
import datetime
import subprocess
import papermill as pm
import luigi
import luigi.contrib.hdfs
import luigi.contrib.webhdfs
from prometheus_client import CollectorRegistry, Gauge
from tasks.common import state_file
from lib.targets import CrawlPackageTarget, CrawlReportTarget, ReportTarget, DatedStateFileTask

logger = logging.getLogger('luigi-interface')


class ListAllFilesOnHDFSToLocalFile(DatedStateFileTask):
    """
    This task lists all files on HDFS (skipping directories).

    As this can be a very large list, it avoids reading it all into memory. It
    parses each line, and creates a CSV line for each.

    It set up to run once a day, as input to downstream reporting or analysis processes.
    """
    date = luigi.DateParameter(default=datetime.date.today())
    task_namespace = "analyse.hdfs"

    tag = 'hdfs'
    name = 'all-files-list'

    total_directories = -1
    total_files = -1
    total_bytes = -1
    total_under_replicated = -1

    @staticmethod
    def fieldnames():
        return ['permissions', 'number_of_replicas', 'userid', 'groupid', 'filesize', 'modified_at', 'filename']

    def run(self):
        command = luigi.contrib.hdfs.load_hadoop_cmd()
        command += ['fs', '-lsr', '/']
        self.total_directories = 0
        self.total_files = 0
        self.total_bytes = 0
        self.total_under_replicated = 0
        with self.output().open('w') as fout:
            # Set up output file:
            writer = csv.DictWriter(fout, fieldnames=ListAllFilesOnHDFSToLocalFile.fieldnames())
            writer.writeheader()
            # Set up listing process
            process = subprocess.Popen(command, stdout=subprocess.PIPE)
            for line in iter(process.stdout.readline, ''):
                if "lsr: DEPRECATED: Please use 'ls -R' instead." in line:
                    logger.warning(line)
                else:
                    permissions, number_of_replicas, userid, groupid, filesize, modification_date, modification_time, filename = line.split(None, 7)
                    filename = filename.strip()
                    timestamp = datetime.datetime.strptime('%s %s' % (modification_date, modification_time), '%Y-%m-%d %H:%M')
                    info = {
                        'permissions' : permissions,
                        'number_of_replicas': number_of_replicas,
                        'userid': userid,
                        'groupid': groupid,
                        'filesize': filesize,
                        'modified_at': timestamp.isoformat(),
                        'filename': filename
                    }
                    # Skip directories:
                    if permissions[0] != 'd':
                        self.total_files += 1
                        self.total_bytes += int(filesize)
                        if number_of_replicas < 3:
                            self.total_under_replicated += 1
                        # Write out as CSV:
                        writer.writerow(info)
                    else:
                        self.total_directories += 1

            # At this point, a temporary file has been written - now we need to check we are okay to move it into place
            if os.path.exists(self.output().path):
                os.rename(self.output().path, "%s.old" % self.output().path)

        # Record a dated flag file to show the work is done.
        with self.dated_state_file().open('w') as fout:
            fout.write(json.dumps(self.get_stats()))

    def get_stats(self):
        return {'dirs' : self.total_directories, 'files': self.total_files, 'bytes' : self.total_bytes,
                'under-replicated' : self.total_under_replicated}

    def get_metrics(self,registry):
        # type: (CollectorRegistry) -> None
        hdfs_service = 'hdfs-0.20'

        g = Gauge('hdfs_files_total_bytes',
                  'Total size of files on HDFS in bytes.',
                  labelnames=['service'], registry=registry)
        g.labels(service=hdfs_service).set(self.total_bytes)

        g = Gauge('hdfs_files_total_count',
                  'Total number of files on HDFS.',
                  labelnames=['service'], registry=registry)
        g.labels(service=hdfs_service).set(self.total_files)

        g = Gauge('hdfs_dirs_total_count',
                  'Total number of directories on HDFS.',
                  labelnames=['service'], registry=registry)
        g.labels(service=hdfs_service).set(self.total_directories)

        g = Gauge('hdfs_under_replicated_files_total_count',
                  'Total number of files on HDFS with less than three copies.',
                  labelnames=['service'], registry=registry)
        g.labels(service=hdfs_service).set(self.total_under_replicated)


class CopyFileListToHDFS(luigi.Task):
    """
    This puts a copy of the file list onto HDFS
    """
    date = luigi.DateParameter(default=datetime.date.today())
    task_namespace = "analyse.hdfs"

    def requires(self):
        return ListAllFilesOnHDFSToLocalFile(self.date)

    def output(self):
        return state_file(self.date,'hdfs','all-files-list.csv.gz', on_hdfs=True)

    def run(self):
        # Make a compressed version of the file:
        gzip_local = '%s.gz' % self.input().path
        with self.input().open('r') as f_in, gzip.open(gzip_local, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

        # Read the compressed file in and write it to HDFS:
        with open(gzip_local,'rb') as f_in, self.output().open('w') as f_out:
            shutil.copyfileobj(f_in, f_out)



