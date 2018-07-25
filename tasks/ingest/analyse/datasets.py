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

    The data looks like this:

201701  aaaee   rukc    2
201701  aaaejd  gaevc   2
201701  aaaepc  eaamyh  2
201701  aaaepf  eaaezd  2
201701  aaaf    3
201701  aaagb   eaae    4
201701  aaagbrl eaaojj  2
201701  aaagghh through 1
201701  aaagh   endless 7
201701  aaagh   here    7

i.e. a mixture of three-column (frequency) and four-column (co-location) data.

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
        dir_path = os.path.dirname(os.path.realpath(__file__))
        return os.path.join(dir_path, "../../jars/ati-word-colocation-0.0.1-SNAPSHOT-job.jar")

    def main(self):
        return "uk.bl.coloc.wa.hadoop.WARCWordColocationAnalysisTool"

    def args(self):
        return ['-i', self.input_file, '-o', self.output()]


if __name__ == '__main__':
    import logging

    logging.getLogger().setLevel(logging.INFO)
    luigi.run(['datasets.GenerateWordColocations', '--input-file', 'warcs-2017-frequent-aa'])
