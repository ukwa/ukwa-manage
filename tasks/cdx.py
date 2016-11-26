import glob
import StringIO
import requests
import json
import luigi.contrib.hdfs
from urlparse import urlparse
from pywb.warc.archiveiterator import DefaultRecordParser
from common import *


def cdx_line(entry, filename):
    out = StringIO.StringIO()
    out.write(entry['urlkey'])
    out.write(' ')
    out.write(entry['timestamp'])
    out.write(' ')
    out.write(entry['url'])
    out.write(' ')
    out.write(entry['mime'])
    out.write(' ')
    out.write(entry['status'])
    out.write(' ')
    out.write(entry['digest'])
    out.write(' - - ')
    out.write(entry['length'])
    out.write(' ')
    out.write(entry['offset'])
    out.write(' ')
    out.write(filename)
    out.write('\n')
    line = out.getvalue()
    out.close()
    return line


class WARCToOutbackCDX(luigi.Task):
    task_namespace = 'cdx'
    job = luigi.EnumParameter(enum=Jobs)
    launch_id = luigi.Parameter()
    filename = luigi.Parameter()
    path = luigi.Parameter()

    def output(self):
        return stats_target(self.job, self.launch_id, os.path.basename(self.path))

    def run(self):
        stats = {
            'record_count' : 0,
            'mime' : {},
            '_content_type' : {},
            'status': {}
        }
        hosts_stats = {}

        entry_iter = DefaultRecordParser(sort=False,
                                         surt_ordered=True,
                                         include_all=False,
                                         verify_http=False,
                                         cdx09=False,
                                         cdxj=False,
                                         minimal=False)(open(self.path, 'rb'))

        session = requests.Session()

        line_count = 0

        for entry in entry_iter:
            # Report progress:
            line_count += 1
            if line_count % 100 == 0:
                self.set_status_message = "Currently at line %i of file %s" % (line_count, self.path)
            #logger.info("Entry: %s" % entry)
            # Create CDX line:
            cdx_11 = cdx_line(entry, self.path)
            stats['record_count'] += 1
            hostname = urlparse(entry['url']).hostname
            host_stats = hosts_stats.get(hostname, {'record_count' : 0, 'mime' : {}, '_content_type' : {}, 'status': {} })
            host_stats['record_count'] += 1
            for key in ['mime', 'status', '_content_type']:
                if entry.has_key(key):
                    counter = stats[key].get(entry[key], 0)
                    counter += 1
                    stats[key][entry[key]] = counter
                    counter = host_stats[key].get(entry[key], 0)
                    counter += 1
                    host_stats[key][entry[key]] = counter
            hosts_stats[hostname] = host_stats

            r = session.post(systems().cdxserver, data=cdx_11.encode('utf-8'))
            #  headers={'Content-type': 'text/plain; charset=utf-8'})
            if r.status_code == 200:
                pass
                #logger.info("POSTed to cdxserver: %s" % cdx_11)
            else:
                logger.error("Failed with %s %s\n%s" % (r.status_code, r.reason, r.text))
                logger.error("Failed submission was: %s" % cdx_11.encode('utf-8'))
                raise Exception("Failed with %s %s\n%s" % (r.status_code, r.reason, r.text))

        with self.output().open('w') as out_file:
            out_file.write('{}'.format(json.dumps({ 'totals': stats , 'by-host': hosts_stats}, indent=4)))


#class IndexWarcs(luigi.WrapperTask):
#    task_namespace = 'cdx'
#    job = luigi.EnumParameter(enum=Jobs)
#    launch_id = luigi.Parameter()
#    paths = luigi.ListParameter()
#
#    def requires(self):
#        for item in self.paths:
#            yield WARCToOutbackCDX(self.job, self.launch, os.path.basename(item), item)
#


class ScanForIndexing(ScanForLaunches):
    """
    This scans for WARCs associated with a particular launch of a given job and CDX indexes them.
    """
    task_namespace = 'scan'
    scan_name = 'cdx'

    def scan_job_launch(self, job, launch):
        # Look in warcs folder for WARCs e.g in /heritrix/output/warcs/{job.name}/{launch_id}
        # n.b. 'viral' don't get indexed, and 'wren' ones should get moved in.
        glob_path = "%s/output/warcs/%s/%s/*.warc.gz" % (h3().local_root_folder, job.name, launch)
        logger.info("PID:%s is looking for warcs: %s" % (os.getpid(), glob_path))
        #warcs = []
        for item in glob.glob(glob_path):
            logger.info("PID:%s is yielding %s" % (os.getpid(), item))
            yield WARCToOutbackCDX(job, launch, os.path.basename(item), item)
            #warcs.append(item)
        #return IndexWarcs(job, launch, warcs)

if __name__ == '__main__':
    luigi.run(['scan.ScanForIndexing', '--date-interval', '2016-11-01-2016-11-10'])

