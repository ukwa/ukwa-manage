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
from lib.targets import CrawlPackageTarget, CrawlReportTarget, ReportTarget

logger = logging.getLogger('luigi-interface')

DEFAULT_BUFFER_SIZE = 1024*1000

"""
Tasks relating to listing HDFS content and reporting on it.
"""


class DatedStateFileTask(luigi.Task):
    """
    This specialisation of a general luigi Task support having two separate files - a small 'dated' overall task
    status file that manages a much larger 'current' file that contains detailed data.
    """

    on_hdfs = False

    def output(self):
        return self.state_file('current')

    def state_file(self, state_date, ext='csv'):
        return state_file(state_date,self.tag,'%s.%s' % (self.name, ext), on_hdfs=self.on_hdfs)

    def dated_state_file(self, ext='json'):
        return self.state_file(self.date, ext=ext)

    def complete(self):
        # Check the dated file exists
        dated_target = self.dated_state_file()
        logger.info("Checking %s exists..." % dated_target.path)
        exists = dated_target.exists()
        if not exists:
            return False
        return True


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


class ListParsedPaths(DatedStateFileTask):
    """
    Identifies in the files on HDFS and classifies them appropriately
    """
    date = luigi.DateParameter(default=datetime.date.today())

    tag = 'hdfs'
    name = 'parsed-paths'

    task_namespace = "analyse.hdfs"

    def requires(self):
        return ListAllFilesOnHDFSToLocalFile(self.date)

    def run(self):
        # Change into this directory:
        dir_path = os.path.dirname(os.path.realpath(__file__))
        os.chdir(dir_path)
        # Record a dated flag file to show the work is done.
        with self.dated_state_file(ext='ipynb').temporary_path() as dated_out_path:
            # Perform this analysis by running the appropriate notebook:
            pm.execute_notebook(
                './hdfs_path_parser.ipynb',
                dated_out_path,
                parameters = dict(
                    file_list_csv = self.input().path,
                    parsed_files_csv = self.output().path
                )
            )


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


class CopyParsedFileListToHDFS(luigi.Task):
    """
    This puts a copy of the file list onto HDFS
    """
    date = luigi.DateParameter(default=datetime.date.today())
    task_namespace = "analyse.hdfs"

    def requires(self):
        return ListParsedPaths(self.date)

    def output(self):
        return state_file(self.date,'hdfs','parsed-files-list.csv.gz', on_hdfs=True)

    def run(self):
        # Make a compressed version of the file:
        gzip_local = '%s.gz' % self.input().path
        with self.input().open('r') as f_in, gzip.open(gzip_local, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

        # Read the compressed file in and write it to HDFS:
        with open(gzip_local,'rb') as f_in, self.output().open('w') as f_out:
            shutil.copyfileobj(f_in, f_out)


class ListEmptyFiles(luigi.Task):
    """
    Takes the full file list and extracts the empty files, as these should be checked.
    """
    date = luigi.DateParameter(default=datetime.date.today())
    task_namespace = "analyse.hdfs"

    def requires(self):
        return ListAllFilesOnHDFSToLocalFile(self.date)

    def output(self):
        return state_file(self.date, 'hdfs', 'empty-files-list.csv')

    def run(self):
        with self.output().open('w') as fout:
            # Set up output file:
            writer = csv.DictWriter(fout, fieldnames=ListAllFilesOnHDFSToLocalFile.fieldnames())
            writer.writeheader()
            with self.input().open('r') as fin:
                reader = csv.DictReader(fin, fieldnames=ListAllFilesOnHDFSToLocalFile.fieldnames())
                for item in reader:
                    # Archive file names:
                    if not item['permissions'].startswith('d') and item['filesize'] == "0":
                        writer.writerow(item)


class ListDuplicateFiles(luigi.Task):
    """
    List all files on HDFS that appear to be duplicates.
    """
    date = luigi.DateParameter(default=datetime.date.today())
    task_namespace = "analyse.hdfs"

    total_unduplicated = 0
    total_duplicated = 0

    def requires(self):
        return ListAllFilesOnHDFSToLocalFile(self.date)

    def output(self):
        return state_file(self.date, 'hdfs', 'duplicate-files-list.tsv')

    def run(self):
        filenames = {}
        with self.input().open('r') as fin:
            reader = csv.DictReader(fin, fieldnames=ListAllFilesOnHDFSToLocalFile.fieldnames())
            for item in reader:
                # Archive file names:
                basename = os.path.basename(item['filename'])
                if basename not in filenames:
                    filenames[basename] = [item['filename']]
                else:
                    filenames[basename].append(item['filename'])

        # And emit duplicates:
        self.total_duplicated = 0
        self.total_unduplicated = 0
        with self.output().open('w') as f:
            for basename in filenames:
                if len(filenames[basename]) > 1:
                    self.total_duplicated += 1
                    f.write("%s\t%i\t%s\n" % (basename, len(filenames[basename]), json.dumps(filenames[basename])))
                else:
                    self.total_unduplicated += 1
        logger.info("Of %i WARC filenames, %i are stored in a single HDFS location." % (len(filenames), self.total_unduplicated))


class ListByCrawl(luigi.Task):
    """
    Identifies in the crawl output files and arranges them by crawl.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    task_namespace = "analyse.report"

    totals = {}
    collections = {}

    def requires(self):
        return ListParsedPaths(self.date)

    #def complete(self):
    #    return False

    def output(self):
        return state_file(self.date, 'hdfs', 'crawl-file-lists.txt')

    def run(self):
        # Go through the data and assemble the resources for each crawl:
        crawls = { }
        unparsed = []
        unparsed_dirs = set()
        with self.input().open('r') as fin:
            reader = csv.DictReader(fin, fieldnames=ListAllFilesOnHDFSToLocalFile.fieldnames())
            # Skip the first line:
            next(reader)

            for p in reader:
                # Process this line:
                collection = 'no-collection'
                stream = 'no-stream'

                # Store the job details:
                if p.recognised and p.job:
                    if p.job not in crawls:
                        crawls[p.job] = {}
                    if p.launch not in crawls[p.job]:
                        crawls[p.job][p.launch] = {}
                    # Store the launch data:
                    if p.launch_datetime:
                        crawls[p.job][p.launch]['date'] = p.launch_datetime.isoformat()
                        crawls[p.job][p.launch]['launch_datetime'] = p.launch_datetime.isoformat()
                        launched = p.launch_datetime.strftime("%d %b %Y")
                    else:
                        launched = '?'
                    crawls[p.job][p.launch]['stream'] = p.stream
                    crawls[p.job][p.launch]['tags'] = ['crawl-%s' % p.stream.name, 'crawl-%s-%s' % (p.stream.name, p.job)]
                    crawls[p.job][p.launch]['total_files'] = 0

                    # Determine the collection and store information at that level:
                    if p.stream == 'frequent' or p.stream == 'domain':
                        collection = 'npld'
                        crawls[p.job][p.launch]['categories'] = ['legal-deposit crawls', '%s crawl' % p.job.split('-')[0]]
                        crawls[p.job][p.launch]['title'] = "NPLD %s crawl, launched %s" % (p.job, launched)
                    elif p.stream == 'selective':
                        collection = 'selective'
                        crawls[p.job][p.launch]['categories'] = ['selective crawls',
                                                                 '%s crawl' % p.job.split('-')[0]]
                        crawls[p.job][p.launch]['title'] = "Selective %s crawl, launched %s" % (p.job, launched)

                    # Append this item:
                    if 'files' not in crawls[p.job][p.launch]:
                        crawls[p.job][p.launch]['files'] = []
                    file_info = {
                        'path': p.file_path,
                        'kind': p.kind,
                        'timestamp': p.timestamp_datetime.isoformat(),
                        'filesize': p['filesize'],
                        'modified_at': p['modified_at']
                    }
                    crawls[p.job][p.launch]['files'].append(file_info)
                    crawls[p.job][p.launch]['total_files'] += 1

                if not p.recognised:
                    #logger.warning("Could not parse: %s" % item['filename'])
                    unparsed.append(p['filename'])
                    unparsed_dirs.add(os.path.dirname(p['filename']))

                # Also count up files and bytes:
                if p.stream:
                    stream = p.stream.name
                if stream not in self.totals:
                    self.totals[stream] = {}
                    self.totals[stream]['all'] = {'count': 0, 'bytes': 0}
                    self.collections[stream] = collection
                # Totals for all files:
                self.totals[stream]['all']['count'] += 1
                self.totals[stream]['all']['bytes'] += int(p['filesize'])
                # Totals broken down by kind:
                if p.kind not in self.totals[stream]:
                    self.totals[stream][p.kind] = { 'count': 0, 'bytes': 0}
                self.totals[stream][p.kind]['count'] += 1
                self.totals[stream][p.kind]['bytes'] += int(p['filesize'])

        # Now emit a file for each, remembering the filenames as we go:
        filenames = []
        for job in crawls:
            for launch in crawls[job]:
                # Grab the stream and just use the name in the dict so we can serialise to JSON:
                stream = crawls[job][launch]['stream']
                crawls[job][launch]['stream'] = stream.name
                # Output a Package file ('versioned' by file count):
                outfile = CrawlPackageTarget(stream, job, launch, crawls[job][launch]['total_files'])
                with outfile.open('w') as f:
                    f.write(json.dumps(crawls[job][launch], indent=2, sort_keys=True))
                filenames.append(outfile.path)
                # Output a Crawl Report file (always the latest version):
                outfile = CrawlReportTarget(stream, job, launch)
                with outfile.open('w') as f:
                    f.write(json.dumps(crawls[job][launch], indent=2, sort_keys=True))
                filenames.append(outfile.path)

        # Output the totals:
        outfile = ReportTarget('data/crawls', 'totals.json')
        with outfile.open('w') as f:
            totals = {
                'totals': self.totals,
                'collections': self.collections
            }
            f.write(json.dumps(totals, indent=2, sort_keys=True))

        # Go through the data and generate some summary stats:
        stats = {}

        # Also emit a list of files that could not be understood:
        outfile = ReportTarget('data/crawls', 'unparsed-file-paths.json')
        with outfile.open('w') as f:
            unparsed_data = {
                'folders': sorted(list(unparsed_dirs)),
                # 'files': unparsed,
                'num_files': len(unparsed)
            }
            f.write(json.dumps(unparsed_data, indent=2, sort_keys=True))

        # Sanity check:
        if len(filenames) == 0:
            raise Exception("No filenames generated! Something went wrong!")

        # Finally, emit the list of output files as the task output:
        with self.output().open('w') as f:
            for output_path in filenames:
                f.write('%s\n' % output_path)

    def get_metrics(self, registry):
        # type: (CollectorRegistry) -> None

        g_b = Gauge('ukwa_files_total_bytes',
                  'Total size of files on HDFS in bytes.',
                  labelnames=['collection', 'stream', 'kind'], registry=registry)
        g_c = Gauge('ukwa_files_total_count',
                  'Total number of files on HDFS.',
                  labelnames=['collection', 'stream', 'kind'], registry=registry)

        # Go through the kinds of data in each collection and
        for stream in self.totals:
            col = self.collections[stream]
            for kind in self.totals[stream]:
                g_b.labels(collection=col, stream=stream, kind=kind).set(self.totals[stream][kind]['bytes'])
                g_c.labels(collection=col, stream=stream, kind=kind).set(self.totals[stream][kind]['count'])


class GenerateHDFSSummaries(luigi.WrapperTask):
    """
    A 'Wrapper Task' that invokes the summaries of HDFS we are interested in.
    """
    task_namespace = "analyse.report"

    def requires(self):
        return [ CopyFileListToHDFS(), ListDuplicateFiles(), ListEmptyFiles(), ListByCrawl(), ListParsedPaths() ]


if __name__ == '__main__':
    import logging

    logging.getLogger().setLevel(logging.INFO)
    #luigi.run(['ListUKWAWebArchiveFilesOnHDFS', '--local-scheduler'])
    luigi.run(['analyse.hdfs.ListParsedPaths', '--local-scheduler', '--date', '2018-02-12'])
    #luigi.run(['ListEmptyFilesOnHDFS', '--local-scheduler'])
