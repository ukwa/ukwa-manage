import luigi
import datetime
import pandas as pd
from tasks.analyse.hdfs_analysis import ListParsedPaths
from tasks.analyse.data_formatters import humanbytes
from tasks.preserve.hdfs_scan_status import GatherBlockScanReports
from lib.targets import ReportTarget, AccessTaskDBTarget


class GenerateHDFSReports(luigi.Task):
    """
    Generate a set of reports based on HDFS content
    """
    date = luigi.DateParameter(default=datetime.date.today())

    task_namespace = "analyse.report"

    def requires(self):
        return {
            'paths' : ListParsedPaths(self.date),
            'scans': GatherBlockScanReports(self.date)
        }

    def output(self):
        return AccessTaskDBTarget(self.task_namespace, self.task_id)

    def run(self):
        # Set up the input file:
        with self.input()['paths'].open('r') as fin:
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
            # Now the same but as file counts:
            out = ReportTarget('content/reports/hdfs', 'total-file-count-by-stream.csv')
            with out.open('w') as f_out:
                # Pandas query:
                df2 = df.groupby([df.collection, df.stream, df.timestamp.dt.year, df.kind]).file_size.count().unstack()
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
                npsm = np.groupby([np.timestamp.dt.to_period('M'), np.stream]).file_size.sum().reset_index()
                npsm.to_csv(f_out,float_format="%.0f", na_rep=0,index=False)

            # NPLD Total:
            out = ReportTarget('content/reports/hdfs', 'npld-total-file-size-by-stream-totals.csv')
            with out.open('w') as f_out:
                totals = np.groupby(np.stream).file_size.sum().reset_index()
                totals = totals.append({'stream': 'total', 'file_size': totals.file_size.sum()}, ignore_index=True)
                totals.file_size = totals.file_size.apply(humanbytes)
                totals = totals.set_index('stream')
                # Same for counts rather than size totals:
                counts = np.groupby(np.stream).file_size.count().reset_index().rename(columns={'file_size': 'file_count'})
                counts = counts.append({'stream': 'total', 'file_count': counts.file_count.sum()}, ignore_index=True)
                counts = counts.set_index('stream')
                # Join the two together into a single table and output:
                totals = totals.join(counts)
                totals.to_csv(f_out)

        # Tag all as done:
        self.output().touch()


if __name__ == '__main__':
    import logging

    logging.getLogger().setLevel(logging.INFO)
    #luigi.run(['ListUKWAWebArchiveFilesOnHDFS', '--local-scheduler'])
    luigi.run(['ingest.report.GenerateHDFSReports', '--local-scheduler', '--date', '2018-02-12'])
