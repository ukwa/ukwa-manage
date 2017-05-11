import os
from lxml import etree
import luigi
import datetime
from shepherd.tasks.common import logger

HERITRIX_CONFIG_ROOT=os.path.realpath(os.path.join(os.path.dirname(__file__),"../../../profiles"))
HERITRIX_PROFILE="%s/profile-domain.cxml" % HERITRIX_CONFIG_ROOT
HERITRIX_EXCLUDE="%s/exclude.txt" % HERITRIX_CONFIG_ROOT
HERITRIX_SHORTENERS="%s/url.shorteners.txt" % HERITRIX_CONFIG_ROOT
HERITRIX_SURTS="%s/surts.txt" % HERITRIX_CONFIG_ROOT


class CreateDomainCrawlJobs(luigi.Task):
    task_namespace = 'dc'
    job = luigi.Parameter()
    date = luigi.DateParameter(default=datetime.datetime.today())
    num_jobs = luigi.Parameter(default=4)
    host = luigi.Parameter()
    amqp_host = luigi.Parameter()

    def create_profile(self, job_id):
        """Creates the CXML content for a H3 job."""
        profile = etree.parse(HERITRIX_PROFILE)
        profile.xinclude()
        cxml = etree.tostring(profile, pretty_print=True, xml_declaration=True, encoding="UTF-8")
        logger.error("HERITRIX_PROFILE %s" % HERITRIX_PROFILE)
        logger.error("self.name %s" % self.name)
        cxml = cxml.replace("REPLACE_JOB_NAME", self.name)
        cxml = cxml.replace("REPLACE_CLAMD_HOST", CLAMD_HOST)
        cxml = cxml.replace("REPLACE_CLAMD_PORT", CLAMD_PORT)
        cxml = cxml.replace("REPLACE_JOB_ROOT", self.name)
        cxml = cxml.replace("REPLACE_HERITRIX_JOBS", HERITRIX_JOBS)
        cxml = cxml.replace("REPLACE_AMQP_HOST", self.amqp_host)
        return cxml





