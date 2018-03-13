#!/usr/bin/env python
# encoding: utf-8
"""
This module summarises the tasks that are to be run monthly.
"""

import luigi
from tasks.ingest.external_data_sources import NominetDomainListToHDFS

class MonthlyIngestTasks(luigi.WrapperTask):
    """
    Monthly ingest tasks.
    """
    def requires(self):
        return [NominetDomainListToHDFS()]


if __name__ == '__main__':
    # Running from Python, but using the Luigi scheduler:
    luigi.run(['MonthlyIngestTasks'])
