#!/usr/bin/env python
# encoding: utf-8
"""
This module summarises the tasks that are to be run daily.
"""

import luigi
from tasks.ingest.list_hdfs_content import CopyFileListToHDFS
from tasks.analyse.hdfs_analysis import GenerateHDFSSummaries
from tasks.analyse.hdfs_reports import GenerateHDFSReports
from tasks.backup.postgresql import BackupProductionW3ACTPostgres, BackupProductionShinePostgres
from tasks.access.search import PopulateCollectionsSolr, GenerateIndexAnnotations, GenerateW3ACTTitleExport
from tasks.access.generate_acl_files import UpdateAccessWhitelist
from tasks.common import state_file


class DailyIngestTasks(luigi.WrapperTask):
    """
    Daily ingest tasks, should generally be a few hours ahead of the access-side tasks (below):
    """
    def requires(self):
        return [BackupProductionW3ACTPostgres(),
                BackupProductionShinePostgres(),
                CopyFileListToHDFS(),
                #GenerateHDFSSummaries(),
                GenerateHDFSReports()]


class DailyAccessTasks(luigi.WrapperTask):
    """
    Daily access tasks. May depend on the ingest tasks, but will usually run from the access server,
    so can't be done in the one job. To be run an hour or so after the :py:DailyIngestTasks.
    """
    def requires(self):
        return [UpdateAccessWhitelist(),
                GenerateIndexAnnotations(),
                PopulateCollectionsSolr(),
                GenerateW3ACTTitleExport()]


if __name__ == '__main__':
    # Running from Python, but using the Luigi scheduler:
    luigi.run(['DailyIngestTasks'])
