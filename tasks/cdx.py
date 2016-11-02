import glob
import StringIO
import requests
import json
import luigi.contrib.hdfs
from pywb.warc.archiveiterator import DefaultRecordParser
from common import *
from move_to_hdfs import ScanForFiles


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
    path = luigi.Parameter()

    session = requests.Session()

    def output(self):
        return stats_target(self.job, self.launch_id, os.path.basename(self.path))

    def run(self):
        stats = {
            'record_count' : 0,
            'mime' : {},
            '_content_type' : {},
            'status': {}
        }

        entry_iter = DefaultRecordParser(sort=False,
                                         surt_ordered=True,
                                         include_all=False,
                                         verify_http=False,
                                         cdx09=False,
                                         cdxj=False,
                                         minimal=False)(open(self.path, 'rb'))

        for entry in entry_iter:
            #logger.info("Entry: %s" % entry)
            cdx_11 = cdx_line(entry, self.path)
            stats['record_count'] += 1
            for key in ['mime', 'status', '_content_type']:
                if entry.has_key(key):
                    counter = stats[key].get(entry[key], 0)
                    counter += 1
                    stats[key][entry[key]] = counter
            r = self.session.post(systems().cdxserver, data=cdx_11.encode('utf-8'))
            #  headers={'Content-type': 'text/plain; charset=utf-8'})
            if r.status_code == 200:
                pass
                #logger.info("POSTed to cdxserver: %s" % cdx_11)
            else:
                logger.error("Failed with %s %s\n%s" % (r.status_code, r.reason, r.text))
                logger.error("Failed submission was: %s" % cdx_11.encode('utf-8'))
                raise Exception("Failed with %s %s\n%s" % (r.status_code, r.reason, r.text))

        with self.output().open('w') as f:
            f.write('{}'.format(json.dumps(stats, indent=4)))


class IndexJobLaunchWARCs(luigi.WrapperTask):
    """
    This scans for WARCs associated with a particular launch of a given job and CDX indexes them.
    """
    task_namespace = 'cdx'
    job = luigi.EnumParameter(enum=Jobs)
    launch_id = luigi.Parameter()
    delete_local = luigi.BoolParameter(default=False)

    def requires(self):
        # Look in warcs and viral for WARCs e.g in /heritrix/output/{warcs|viral}/{job.name}/{launch_id}
        for item in glob.glob("%s/output/warcs/%s/%s/*.warc.gz" % (h3().local_root_folder, self.job.name, self.launch_id)):
            yield WARCToOutbackCDX(self.job, self.launch_id, item)
        # Look in /heritrix/output/wren too:
        for item in glob.glob("%s/output/wren/*-%s-%s-*.warc.gz" % (h3().local_root_folder, self.job.name, self.launch_id)):
            yield WARCToOutbackCDX(self.job, self.launch_id, item)


class ScanForIndexing(ScanForFiles):
    task_namespace = 'cdx'

    def scan_job_launch(self, job, launch):
        return IndexJobLaunchWARCs(job, launch)

if __name__ == '__main__':
    luigi.run(['cdx.ScanForIndexing', '--date-interval', '2016-11-01-2016-11-10'])

