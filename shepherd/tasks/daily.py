import luigi
from shepherd.tasks.process.hadoop.hasher import GenerateHDFSSummaries
from shepherd.tasks.access.turing import ListFilesToUploadToAzure
from shepherd.tasks.backup.postgresql import BackupProductionW3ACTPostgres


class DailyIngestTasks(luigi.WrapperTask):
    """
    Daily ingest tasks, should generally be a few hours ahead of the access-side tasks (below):
    """
    def requires(self):
        return [ BackupProductionW3ACTPostgres(), GenerateHDFSSummaries() ]


class DailyAccessTasks(luigi.WrapperTask):
    """
    Daily access tasks. May depend on the ingest tasks, but will usually run from the access server,
    so can't be done in the one job. To be run an hour or so after the :py:DailyIngestTasks.
    """
    def requires(self):
        return [ ListFilesToUploadToAzure() ]