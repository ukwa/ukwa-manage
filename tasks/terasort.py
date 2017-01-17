import luigi
import luigi.contrib.hdfs
import luigi.contrib.hadoop_jar

DEFAULT_TERASORT_IN = '/tmp/terasort-in'
DEFAULT_TERASORT_OUT = '/tmp/terasort-out'


class TeraGen(luigi.contrib.hadoop_jar.HadoopJarJobTask):
    """
    Runs TeraGen, by default with 100KB of data (1000 records)
    """

    records = luigi.Parameter(default="1000",
                              description="Number of records, each record is 100 Bytes")
    terasort_in = luigi.Parameter(default=DEFAULT_TERASORT_IN,
                                  description="directory to store terasort input into.")

    def output(self):
        """
        Returns the target output for this task.
        In this case, a successful execution of this task will create a file in HDFS.
        :return: the target output for this task.
        :rtype: object (:py:class:`~luigi.target.Target`)
        """
        return luigi.contrib.hdfs.HdfsTarget(self.terasort_in)

    def jar(self):
        return "./jars/hadoop-mapreduce-examples-2.7.0.jar"
        #return "/usr/local/hadoop-2.7.0/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.0.jar"

    def main(self):
        return "teragen"

    def args(self):
        # First arg is 10B -- each record is 100bytes
        return [self.records, self.output()]

if __name__ == '__main__':
    luigi.run(['TeraGen'])