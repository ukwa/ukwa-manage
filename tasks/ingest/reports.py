import luigi
import datetime
import pandas as pd
from tasks.ingest.listings import ListParsedPaths
from lib.targets import ReportTarget, TaskTarget


# Utility for pretty-printing in e.g. TB (not TiB):
def humanbytes(B):
    '''Return the given bytes as a human friendly KB, MB, GB, or TB string'''
    B = float(B)
    KB = float(1000)
    MB = float(KB ** 2)
    GB = float(KB ** 3)
    TB = float(KB ** 4)

    if B < KB:
        return '{0} {1}'.format(B,'Bytes' if 0 == B > 1 else 'Byte')
    elif KB <= B < MB:
        return '{0:.2f} KB'.format(B/KB)
    elif MB <= B < GB:
        return '{0:.2f} MB'.format(B/MB)
    elif GB <= B < TB:
        return '{0:.2f} GB'.format(B/GB)
    elif TB <= B:
        return '{0:.2f} TB'.format(B/TB)


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
                df2.to_csv(f_out,float_format="%.0f")

            # Focus on NPLD:
            np = df.loc[df.collection == 'npld'].loc[df.kind.isin(['warcs', 'crawl-logs', 'viral'])].reset_index()

            # NPLD By year, humanbytes:
            out = ReportTarget('content/reports/hdfs', 'npld-total-file-size-by-stream-per-year.csv')
            with out.open('w') as f_out:
                npsy = np.groupby([np.timestamp.dt.year, np.stream]).file_size.sum().apply(humanbytes).unstack()
                npsy.to_csv(f_out)

            # NPLD By month:
            out = ReportTarget('content/reports/hdfs', 'npld-total-file-size-by-stream-per-month.csv')
            with out.open('w') as f_out:
                npsm = np.groupby([np.timestamp.dt.to_period('M'), np.stream]).file_size.sum().unstack()
                npsm.to_csv(f_out,float_format="%.0f")

            # NPLD Total:
            out = ReportTarget('content/reports/hdfs', 'npld-total-file-size-by-stream-totals.csv')
            with out.open('w') as f_out:
                totals = np.groupby(np.stream).file_size.sum().reset_index()
                totals = totals.append({'stream': 'total', 'file_size': totals.file_size.sum()}, ignore_index=True)
                totals.file_size = totals.file_size.apply(humanbytes)
                totals.to_csv(f_out,index=False)


            # Tag all as done:
        with self.output().open('w') as f_out:
            f_out.write("DONE")


if __name__ == '__main__':
    import logging

    logging.getLogger().setLevel(logging.INFO)
    #luigi.run(['ListUKWAWebArchiveFilesOnHDFS', '--local-scheduler'])
    luigi.run(['ingest.report.GenerateHDFSReports', '--local-scheduler', '--date', '2018-02-12'])
