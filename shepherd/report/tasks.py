import os
import luigi
import luigi.contrib.hdfs
import luigi.contrib.hadoop_jar


class GenerateWarcStats(luigi.contrib.hadoop_jar.HadoopJarJobTask):
    """
    Generates the Warc stats by reading in each file and splitting the stream into entries.
    As this uses the stream directly and so data-locality is preserved.

    Parameters:
        input_file: The file (locally) that contains the list of WARC files to process
    """
    input_file = luigi.Parameter()

    def output(self):
        out_name = "%s-stats.tsv" % os.path.splitext(self.input_file)[0]
        return luigi.contrib.hdfs.HdfsTarget(out_name, format=luigi.contrib.hdfs.PlainDir)

    #def requires(self):
    #    return ExternalFilesFromList(self.input_file)

    def jar(self):
        return "../jars/warc-hadoop-recordreaders-2.2.0-BETA-7-SNAPSHOT-job.jar"

    def main(self):
        return "uk.bl.wa.hadoop.mapreduce.warcstats.WARCStatsTool"

    def args(self):
        return [self.input_file, self.output()]


if __name__ == '__main__':
    luigi.run(['GenerateWarcStats', '--input-file', 'daily-warcs-test.txt', '--local-scheduler'])
