import luigi
import datetime
import pandas as pd
from tasks.ingest.listings import ListParsedPaths
from lib.targets import ReportTarget, TaskTarget


class GenerateHDFSReports(luigi.Task):
    """
    Generate a set of reports based on HDFS content
    """
    date = luigi.DateParameter(default=datetime.date.today())

    task_namespace = "ingest.report"

    def requires(self):
        return ListParsedPaths(self.date)

    def output(self):
        return TaskTarget('reports', 'hdfs-reports', self.date)

    def run(self):
        # Set up the input file:
        with self.input().open('r') as fin:
            df = pd.read_csv(fin)
            # Interpret timestamp as a date:
            df.timestamp = pd.to_datetime(df.timestamp)
            # Ignore the to-be-deleted data:
            df = df.loc[df['kind'] != 'to-be-deleted']

            # Write out the per-collection report:
            out = ReportTarget('content/reports/hdfs', 'total-file-size-by-stream.csv')
            with out.open('w') as f_out:
                # Pandas query:
                df2 = df.groupby([df.collection, df.stream, df.timestamp.dt.year, df.kind]).file_size.sum().unstack()
                # Output the result as CSV:
                df2.to_csv(f_out)

        # Tag all as done:
        #with self.output().open('w') as f_out:
        #    f_out.write("DONE")


if __name__ == '__main__':
    import logging

    logging.getLogger().setLevel(logging.INFO)
    #luigi.run(['ListUKWAWebArchiveFilesOnHDFS', '--local-scheduler'])
    luigi.run(['ingest.report.GenerateHDFSReports', '--local-scheduler', '--date', '2018-02-12'])
