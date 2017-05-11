import requests
from lxml import etree
import xml.etree.ElementTree as ET
from requests.auth import HTTPDigestAuth
import pprint
from hapy import Hapy
from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)

"""
Extended version of Hapy with some utilities added on.
"""
class HapyX(Hapy):

    def __init__(self, base_url, username=None, password=None, insecure=True, timeout=None):
        Hapy.__init__(self, base_url, username, password, insecure, timeout)

    def status(self, job=""):
        info = self.get_job_info(job)
        if info.has_key('job'):
            status = info['job'].get("crawlControllerState", "")
        else:
            status = ""
        return status

    def list_jobs(self, status=None):
        r = self._http_get(self.base_url)
        xml = etree.fromstring(r.content)
        if status is None:
            return [job.find("shortName").text for job in xml.xpath("//jobs/value")]
        else:
            return [job.find("shortName").text for job in xml.xpath("//jobs/value[./crawlControllerState = '%s']" % status)]

    def get_launch_id(self, job=""):
        raw, html = self.execute_script(job,"groovy","rawOut.println( appCtx.getCurrentLaunchId() );")
        if raw:
            raw = raw.strip()
        return raw

    def get_seeds( self, job ):
        url = "%s/job/%s/jobdir/latest/seeds.txt" % ( self.host, job )
        r = requests.get( url, auth=HTTPDigestAuth( self.user, self.passwd ), verify=self.verify )
        seeds = [ seed.strip() for seed in r.iter_lines() ]
        for i, seed in enumerate( seeds ):
            if seed.startswith( "#" ):
                return seeds[ 0:i ]
        return seeds

    def empty_frontier( self, job ):
        script = "count = job.crawlController.frontier.deleteURIs( \".*\", \"^.*\" )\nrawOut.println count"
        xml = self.execute_script(job, "groovy", script)
        tree = ET.fromstring( xml.content )
        return tree.find( "rawOutput" ).text.strip()

    def launch_from_latest_checkpoint(self, job):
        info = self.get_job_info(job)
        if info.has_key('job'):
            checkpoints = info['job'].get("checkpointFiles").get("value", [])
        else:
            checkpoints = []

        if len(checkpoints) == 0:
            logger.info("No checkpoint found. Lauching as new job...")
            self.launch_job(job)
        else:
            # Select the most recent checkpoint:
            checkpoint = checkpoints[0]
            logger.info("Launching from checkpoint %s..." %  checkpoint)
            # And launch:
            self._http_post(
                url='%s/job/%s' % (self.base_url, job),
                data=dict(
                    action='launch',
                    checkpoint=checkpoint
                ),
                code=303
            )



