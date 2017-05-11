import luigi
from tasks.process.log_analysis import GenerateCrawlLogReports
from tasks.common import logger


class GenerateCrawlReport(luigi.Task):
    """
    """
    task_namespace = 'scan'
    job = luigi.Parameter()
    launch = luigi.Parameter()

    #def requires(self):
    #    return GenerateCrawlLogReports(self.job, self.launch)


if __name__ == '__main__':
    luigi.run(['report.GenerateCrawlReport', '--local-scheduler'])
    #luigi.run(['GenerateCrawlReport', '--date-interval', "2017-01-13-2017-01-18", '--local-scheduler'])
