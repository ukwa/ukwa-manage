import os
import re
import csv
import json
import gzip
import shutil
import logging
import datetime
import subprocess
import luigi
import luigi.contrib.hdfs
import luigi.contrib.webhdfs
from prometheus_client import CollectorRegistry, Gauge
from tasks.common import state_file, report_file

logger = logging.getLogger('luigi-interface')

DEFAULT_BUFFER_SIZE = 1024*1000

csv_fieldnames = ['permissions', 'number_of_replicas', 'userid', 'groupid', 'filesize', 'modified_at', 'filename']


class ListAllFilesOnHDFSToLocalFile(luigi.Task):
    """
    This task lists all files on HDFS (skipping directories).

    As this can be a very large list, it avoids reading it all into memory. It
    parses each line, and creates a CSV line for each.

    It set up to run once a day, as input to downstream reporting or analysis processes.
    """
    date = luigi.DateParameter(default=datetime.date.today())
    task_namespace = "hdfs"

    total_directories = -1
    total_files = -1
    total_bytes = -1
    total_under_replicated = -1

    def output(self):
        return self.state_file('current')

    def state_file(self, state_date, ext='csv'):
        return state_file(state_date,'hdfs','all-files-list.%s' % ext, on_hdfs=False)

    def dated_state_file(self):
        return self.state_file(self.date, ext='json')

    def complete(self):
        # Check the dated file exists
        dated_target = self.dated_state_file()
        logger.info("Checking %s exists..." % dated_target.path)
        exists = dated_target.exists()
        if not exists:
            return False
        return True

    def run(self):
        command = luigi.contrib.hdfs.load_hadoop_cmd()
        command += ['fs', '-lsr', '/']
        self.total_directories = 0
        self.total_files = 0
        self.total_bytes = 0
        self.total_under_replicated = 0
        with self.output().open('w') as fout:
            # Set up output file:
            writer = csv.DictWriter(fout, fieldnames=csv_fieldnames)
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
    task_namespace = "hdfs"

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


class ListEmptyFiles(luigi.Task):
    """
    Takes the full file list and extracts the empty files, as these should be checked.
    """
    date = luigi.DateParameter(default=datetime.date.today())
    task_namespace = "hdfs"

    def requires(self):
        return ListAllFilesOnHDFSToLocalFile(self.date)

    def output(self):
        return state_file(self.date, 'hdfs', 'empty-files-list.csv')

    def run(self):
        with self.output().open('w') as fout:
            # Set up output file:
            writer = csv.DictWriter(fout, fieldnames=csv_fieldnames)
            writer.writeheader()
            with self.input().open('r') as fin:
                reader = csv.DictReader(fin, fieldnames=csv_fieldnames)
                for item in reader:
                    # Archive file names:
                    if not item['permissions'].startswith('d') and item['filesize'] == "0":
                        writer.writerow(item)


class ListUKWAFiles(luigi.Task):
    """
    Takes the full WARC list and filters UKWA content by folder:
    """
    date = luigi.DateParameter(default=datetime.date.today())
    task_namespace = "hdfs"

    def requires(self):
        return ListAllFilesOnHDFSToLocalFile(self.date)

    def output(self):
        return state_file(self.date, 'hdfs', 'ukwa-files-list.csv')

    def run(self):
        with self.output().open('w') as f:
            writer = csv.DictWriter(f, fieldnames=csv_fieldnames)
            writer.writeheader()
            with self.input().open('r') as fin:
                reader = csv.DictReader(fin, fieldnames=csv_fieldnames)
                for item in reader:
                    item['filename'] = item['filename'].strip()
                    # Archive file names:
                    if item['filename'].startswith('/data/') or item['filename'].startswith('/heritrix/'):
                        writer.writerow(item)


class ListWebArchiveFiles(luigi.Task):
    """
    Takes the full file list and strips it down to just the WARCs and ARCs
    """
    date = luigi.DateParameter(default=datetime.date.today())
    task_namespace = "hdfs"

    def requires(self):
        return ListAllFilesOnHDFSToLocalFile(self.date)

    def output(self):
        return state_file(self.date, 'hdfs', 'warc-files-list.csv')

    def run(self):
        with self.output().open('w') as f:
            writer = csv.DictWriter(f, fieldnames=csv_fieldnames)
            writer.writeheader()
            with self.input().open('r') as fin:
                reader = csv.DictReader(fin, fieldnames=csv_fieldnames)
                for item in reader:
                    item['filename'] = item['filename'].strip()
                    # Archive file names:
                    if item['filename'].endswith('.warc.gz') or item['filename'].endswith('.arc.gz') \
                            or item['filename'].endswith('.warc') or item['filename'].endswith('.arc'):
                        writer.writerow(item)


class ListUKWAWebArchiveFiles(luigi.Task):
    """
    Takes the full WARC list and filters UKWA content by folder:
    """
    date = luigi.DateParameter(default=datetime.date.today())
    task_namespace = "hdfs"

    def requires(self):
        return ListWebArchiveFiles(self.date)

    def output(self):
        return state_file(self.date, 'hdfs', 'warc-ukwa-files-list.csv')

    def run(self):
        with self.output().open('w') as f:
            writer = csv.DictWriter(f, fieldnames=csv_fieldnames)
            writer.writeheader()
            with self.input().open('r') as fin:
                reader = csv.DictReader(fin, fieldnames=csv_fieldnames)
                for item in reader:
                    # Archive file names:
                    if item['filename'].startswith('/data/') or item['filename'].startswith('/heritrix/'):
                        writer.writerow(item)


class ListUKWAFilesByCollection(luigi.Task):
    """
    Takes the full file list and filters UKWA content by collection 'npld' or 'selective':
    """
    date = luigi.DateParameter(default=datetime.date.today())
    subset = luigi.Parameter(default='npld')
    task_namespace = "hdfs"

    total_files = 0
    total_bytes = 0
    total_warc_files = 0
    total_warc_bytes = 0

    def requires(self):
        return ListUKWAFiles(self.date)

    def output(self):
        return state_file(self.date, 'hdfs', 'ukwa-%s-files-list.csv' % self.subset)

    def run(self):
        with self.output().open('w') as f:
            writer = csv.DictWriter(f, fieldnames=csv_fieldnames)
            writer.writeheader()
            with self.input().open('r') as fin:
                reader = csv.DictReader(fin, fieldnames=csv_fieldnames)
                for item in reader:
                    # Archive file names:
                    if (self.subset == 'selective' and item['filename'].startswith('/data/')) \
                            or (self.subset == 'npld' and item['filename'].startswith('/heritrix/')):
                        self.total_files += 1
                        self.total_bytes += int(item['filesize'])
                        if item['filename'].endswith('.warc.gz'):
                            self.total_warc_files += 1
                            self.total_warc_bytes += int(item['filesize'])
                        writer.writerow(item)

    def get_metrics(self, registry):
        # type: (CollectorRegistry) -> None
        col = self.subset

        g = Gauge('ukwa_files_total_bytes',
                  'Total size of files on HDFS in bytes.',
                  labelnames=['collection', 'kind'], registry=registry)
        g.labels(collection=col, kind='all').set(self.total_bytes)
        g.labels(collection=col, kind='warcs').set(self.total_warc_bytes)

        g = Gauge('ukwa_files_total_count',
                  'Total number of files on HDFS.',
                  labelnames=['collection', 'kind'], registry=registry)
        g.labels(collection=col, kind='all').set(self.total_files)
        g.labels(collection=col, kind='warcs').set(self.total_warc_files)

        # Old metrics (deprected, will be removed shortly):
        g = Gauge('ukwa_warc_files_total_bytes',
                  'Total size of files on HDFS in bytes.',
                  labelnames=['collection'], registry=registry)
        g.labels(collection=col).set(self.total_warc_bytes)

        g = Gauge('ukwa_warc_files_total_count',
                  'Total number of files on HDFS.',
                  labelnames=['collection'], registry=registry)
        g.labels(collection=col).set(self.total_warc_files)


class ListDuplicateWebArchiveFiles(luigi.Task):
    """
    Takes the full WARC list and filters UKWA content by folder:
    """
    date = luigi.DateParameter(default=datetime.date.today())
    collection = luigi.Parameter(default='all')
    task_namespace = "hdfs"

    total_unduplicated = 0
    total_duplicated = 0

    def requires(self):
        if self.collection == 'ukwa':
            return ListUKWAWebArchiveFiles(self.date)
        elif self.collection == 'all':
            return ListWebArchiveFiles(self.date)
        else:
            raise Exception("Unrecognised collection parameter! %s non known!" % self.collection)

    def output(self):
        return state_file(self.date, 'hdfs', 'warc-%s-duplicate-files-list.tsv' % self.collection)

    def run(self):
        filenames = {}
        with self.input().open('r') as fin:
            reader = csv.DictReader(fin, fieldnames=csv_fieldnames)
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
    Loads in the crawl files and arranges them by crawl.
    """
    date = luigi.DateParameter(default=datetime.date.today())
    stream = luigi.Parameter(default='npld')

    def requires(self):
        return ListUKWAFilesByCollection(self.date)

    #def complete(self):
    #    return False

    def output(self):
        return state_file(self.date, 'hdfs', 'crawl-file-lists.txt')

    def get_path_for(self, stream, job, launch, count):
        return report_file(None, 'content/crawls', '%s/%s/%s/%08d.md' % (stream,job,launch,count))

    def run(self):
        # Go through the data and assemble the resources for each crawl:
        crawls = { }
        unparsed = []
        unparsed_dirs = set()
        with self.input().open('r') as fin:
            reader = csv.DictReader(fin, fieldnames=csv_fieldnames)
            for item in reader:
                # Archive file names:
                file_path = item['filename']
                # Look for different filename patterns:
                mfc = re.search('/heritrix/output/(warcs|viral|logs)/([a-z\-0-9]+)[-/]([0-9]{12,14})/([^\/]+)', file_path)
                mdc = re.search('/heritrix/output/(warcs|viral|logs)/(dc|crawl)[0-3]\-([0-9]{8}|[0-9]{14})/([^\/]+)', file_path)
                if mdc:
                    (kind, job, launch, filename) = mdc.groups()
                    job = 'domain' # Overriding old job name.
                    # Cope with variation in folder naming - all DC crawlers launched on the same day:
                    if len(launch) > 8:
                        launch = launch[0:8]
                    launch_datetime = datetime.datetime.strptime(launch, "%Y%m%d")
                elif mfc:
                    (kind, job, launch, filename) = mfc.groups()
                    launch_datetime = datetime.datetime.strptime(launch, "%Y%m%d%H%M%S")
                else:
                    # print("Could not parse: %s" % file_path)
                    logger.warning("Could not parse: %s" % file_path)
                    unparsed.append(file_path)
                    unparsed_dirs.add(os.path.dirname(file_path))
                    continue

                # Attempt to parse file timestamp out of filename:
                m = re.search('^.*-([12][0-9]{16})-.*\.warc\.gz$', filename)
                if m:
                    file_timestamp = datetime.datetime.strptime(m.group(1), "%Y%m%d%H%M%S%f")
                else:
                    # fall back on launch timestamp:
                    file_timestamp = launch_datetime

                # Store the job details:
                if job not in crawls:
                    #print(file_datestamp, kind, job, launch, filename)
                    crawls[job] = {}
                if launch not in crawls[job]:
                    crawls[job][launch] = {}
                    crawls[job][launch]['date'] = launch_datetime.isoformat()
                    crawls[job][launch]['categories'] = ['legal-deposit crawls', '%s crawl' % job.split('-')[0]]
                    crawls[job][launch]['tags'] = ['crawl/npld/%s' % job, 'crawl/npld/%s/%s' % (job, launch)]
                    crawls[job][launch]['total_files'] = 0
                    crawls[job][launch]['launch_datetime'] = launch_datetime.isoformat()
                # Append this item:
                if 'files' not in crawls[job][launch]:
                    crawls[job][launch]['files'] = []
                file_info = {
                    'path': file_path,
                    'kind': kind,
                    'timestamp': file_timestamp.isoformat(),
                    'filesize': item['filesize'],
                    'modified_at': item['modified_at']
                }
                crawls[job][launch]['files'].append(file_info)
                crawls[job][launch]['total_files'] += 1

        # Now emit a file for each, remembering the filenames as we go:
        filenames = []
        for job in crawls:
            for launch in crawls[job]:
                #print(job, launch, crawls[job][launch]['total_files'], )
                outfile = self.get_path_for(self.stream,job, launch, crawls[job][launch]['total_files'])
                launch_datetime = datetime.datetime.strptime(crawls[job][launch]['launch_datetime'], "%Y-%m-%dT%H:%M:%S")
                launched = launch_datetime.strftime("%d %b %Y")
                crawls[job][launch]['title'] = "NPLD %s crawl, launched %s (file count %i)" % (job, launched, crawls[job][launch]['total_files'])
                with outfile.open('w') as f:
                    f.write(json.dumps(crawls[job][launch], indent=2, sort_keys=True))
                filenames.append(outfile.path)

        # Also emit a list of files that could not be understood:
        outfile = report_file(None, 'data/crawls', 'unparsed-file-paths.json')
        with outfile.open('w') as f:
            unparsed_data = {
                'folders': sorted(list(unparsed_dirs)),
#                'files': unparsed,
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


class GenerateHDFSSummaries(luigi.WrapperTask):
    task_namespace = "hdfs"

    def requires(self):
        return [ CopyFileListToHDFS(), ListUKWAWebArchiveFiles(), ListDuplicateWebArchiveFiles(), ListEmptyFiles(),
                 ListByCrawl() ]


class PrintSomeLines(luigi.Task):
    """
    An example to try to get things working:
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return ListAllFilesOnHDFSToLocalFile(self.date)

    def output(self):
        return state_file(self.date, 'hdfs', 'empty-files-list.csv')

    def run(self):
        with self.input().open('r') as fin:
            reader = csv.DictReader(fin, fieldnames=csv_fieldnames)
            for item in reader:
                print(item)
                break



if __name__ == '__main__':
    import logging

    logging.getLogger().setLevel(logging.INFO)
    #luigi.run(['ListUKWAWebArchiveFilesOnHDFS', '--local-scheduler'])
    luigi.run(['ListWarcsByDate', '--local-scheduler', '--target-date', '2018-02-10'])
    #luigi.run(['ListEmptyFilesOnHDFS', '--local-scheduler'])
