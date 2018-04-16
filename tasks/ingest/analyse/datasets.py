import os
import luigi
import luigi.contrib.hdfs
import luigi.contrib.hadoop_jar

"""
Tasks used to generate datasets for researchers.

"""


class GenerateWordColocations(luigi.contrib.hadoop_jar.HadoopJarJobTask):
    """
    This generates word co-location data for a batch of WARCs.

    Parameters:
        input_file: A local file that contains the list of WARC files to process
    """
    input_file = luigi.Parameter()
    task_namespace = "datasets"

    def output(self):
        out_name = "%s-word-coloc.tsv" % os.path.splitext(self.input_file)[0]
        return luigi.contrib.hdfs.HdfsTarget(out_name, format=luigi.contrib.hdfs.Plain)

    #def requires(self):
    #    return tasks.report.crawl_summary.GenerateWarcList(self.input_file)

    def jar(self):
        return "../jars/ati-word-colocation-0.0.1-SNAPSHOT-job.jar"

    def main(self):
        return "uk.bl.coloc.wa.hadoop.WARCWordColocationAnalysisTool"

    def args(self):
        return ['-i', self.input_file, '-o', self.output()]


if __name__ == '__main__':
    import logging

    logging.getLogger().setLevel(logging.INFO)
    luigi.run(['datasets.GenerateWordColocations', 'daily-warcs-test.txt'])
