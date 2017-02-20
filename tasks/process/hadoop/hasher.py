import os
import datetime
import logging
import luigi
import luigi.contrib.hdfs
import luigi.contrib.hadoop_jar

logger = logging.getLogger('luigi-interface')


class ListAllFilesOnHDFS(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today())

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget('/state/hdfs/filelist-%s.txt' % self.date.strftime("%Y-%m-%d"))

    def run(self):
        client = luigi.contrib.hdfs.get_autoconfig_client()
        for (path, dirs, files) in client.walk('/', depth=10):
            #print(path, dirs, files)
            for filename in files:
                print(os.path.join(path, filename))



class GenerateWarcHashes(luigi.contrib.hadoop_jar.HadoopJarJobTask):
    """
    Generates the SHA-512 hashes for the WARCs directly on HDFS.

    Parameters:
        input_file: A local file that contains the list of WARC files to process
    """
    input_file = luigi.Parameter()

    def output(self):
        out_name = "%s-sha512.tsv" % os.path.splitext(self.input_file)[0]
        return luigi.contrib.hdfs.HdfsTarget(out_name, format=luigi.contrib.hdfs.Plain)

    #def requires(self):
    #    return tasks.report.crawl_summary.GenerateWarcList(self.input_file)

    def jar(self):
        return "../jars/warc-hadoop-recordreaders-2.2.0-BETA-7-SNAPSHOT-job.jar"

    def main(self):
        return "uk.bl.wa.hadoop.mapreduce.hash.HdfsFileHasher"

    def args(self):
        return [self.input_file, self.output()]


if __name__ == '__main__':
    luigi.run(['ListAllFilesOnHDFS', '--local-scheduler'])
#    luigi.run(['GenerateWarcHashes', 'daily-warcs-test.txt'])
