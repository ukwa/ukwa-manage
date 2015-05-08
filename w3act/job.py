#!/usr/bin/env python

"""
Methods for controlling w3act jobs in Heritrix.
"""

import os
import json
import time
import shutil
import logging
import heritrix
import requests
from lxml import etree
from w3act import settings
from urlparse import urlparse
from w3act.lib import ACT, unique_list
from xml.etree.ElementTree import ParseError

requests.packages.urllib3.disable_warnings()

logger = logging.getLogger("w3act.%s" % __name__)
handler = logging.FileHandler("%s/%s.log" % (settings.LOG_ROOT, __name__))
formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

#Try to set logging output for all modules.
logging.root.setLevel(logging.WARNING)
logging.getLogger("").addHandler(handler)


mandatory_fields = ["field_url", "field_depth", "field_scope", settings.W3ACT_JOB_FIELD]
depth_sheets = {"capped_large": "higherLimit", "deep": "noLimit"}
scope_sheets = {"resource": "resourceScope", "plus1": "plus1Scope", "subdomains": "subdomainsScope"}


def to_surt(url):
        parsed = urlparse(url).netloc
        authority = parsed.split(".")
        authority.reverse()
        return "http://(%s," % ",".join(authority)


def get_surt_association_script(surt, sheet):
    """Creates the beanshell script for a SURT->Sheet association."""
    return "appCtx.getBean(\"sheetOverlaysManager\").addSurtAssociation(\"%s\", \"%s\" );" % (surt, sheet)


def get_depth_scripts(seeds, depth):
    """Creates a list of beanshell commands for seed/depth."""
    if depth not in depth_sheets.keys():
        return []
    sheet = depth_sheets[depth.lower()]
    script = [get_surt_association_script(to_surt(seed), sheet) for seed in seeds]
    return script


def get_scope_scripts(seeds, scope):
    """Creates a list of beanshell commands for seed/scope."""
    script = []
    if scope in scope_sheets.keys():
        for seed in seeds:
            surt = to_surt(seed)
            sheet = scope_sheets[scope]
            script.append(get_surt_association_script(surt, sheet))
            logger.info("Setting scope for %s to %s" % (surt, sheet))
    return script


def get_blocking_scripts():
    """Blocks access to w3act's 'nevercrawl' targets."""
    w = ACT()
    j = w.get_ld_export("nevercrawl")
    blocked_urls = unique_list([to_surt(u["url"]) for t in j for u in t["fieldUrls"]])
    script = [get_surt_association_script(surt, "blockAll") for surt in blocked_urls]
    return script


def get_relevant_fields(nodes):
    """Retrieves subset of a Target's fields."""
    targets = []
    for node in nodes:
        target_info = { key: node[key] for key in settings.W3ACT_FIELDS }
        target_info["seeds"] = [u["url"] for u in node["fieldUrls"]]
        if "watched" in node.keys():
            target_info["watched"] = node["watched"]
            target_info["watchedTarget"] = node["watchedTarget"]
        targets.append(target_info)
    return targets


class W3actJob(object):
    """Represents a Heritrix job for w3act."""

    def get_name(self, url_field):
        """Inconsistent 'url' values."""
        if "/" in url_field:
            fields = url_field.split("/")
            return "%s-%s" % (fields[-2], fields[-1])
        else:
            return url_field

    def __init__(self, w3act_targets, name=None, seeds=None, directory=None, heritrix=None, setup=True):
        if name is None:
            self.name = self.get_name(w3act_targets[0][settings.W3ACT_JOB_FIELD])
        else:
            self.name = name
        if seeds is None:
            self.seeds = [u["url"] for t in w3act_targets for u in t["fieldUrls"]]
        else:
            self.seeds = seeds
        if setup:
            logger.debug("Configuring directory for job '%s'." % self.name)
            self.info = get_relevant_fields(w3act_targets)
            self.setup_job_directory()
        else:
            self.info = w3act_targets
            self.job_dir = directory
        if heritrix is not None:
            self.heritrix = heritrix
            self.heritrix.add(self.job_dir)

    @staticmethod
    def from_directory(path, heritrix=None):
        """Build a job from an existing directory."""
        logger.debug("Building job from directory: %s" % path)
        name = os.path.basename(path)
        if os.path.exists("%s/latest/w3act-info.json" % path):
            with open("%s/latest/w3act-info.json" % path, "rb") as i:
                info = json.loads(i.read())
        else:
            info = []
        with open("%s/latest/seeds.txt" % path, "rb") as i:
            seeds = [l.strip() for l in i if not l.startswith("#") and len(l.strip()) > 0]
        job = W3actJob(info, name=name, seeds=seeds, directory=path, heritrix=heritrix, setup=False)
        return job


    def setup_heritrix(self, api=None, host=None, port=None, user="admin", passwd="bl_uk"):
        if api is not None:
            self.heritrix = api
        else:
            self.heritrix = heritrix.API(host="https://%s:%s/engine" % (host, port), user=user, passwd=passwd, verbose=False, verify=False)
        self.heritrix.add(self.job_dir)


    def create_profile(self):
        """Creates the CXML content for a H3 job."""
        profile = etree.parse(settings.HERITRIX_PROFILE)
        profile.xinclude()
        cxml = etree.tostring(profile, pretty_print=True, xml_declaration=True, encoding="UTF-8")
        cxml = cxml.replace("REPLACE_JOB_NAME", self.name)
        if self.name in settings.CLAMD_PORTS.keys():
            cxml = cxml.replace("REPLACE_CLAMD_PORT", settings.CLAMD_PORTS[self.name])
        else:
            cxml = cxml.replace("REPLACE_CLAMD_PORT", settings.CLAMD_DEFAULT_PORT)
        cxml = cxml.replace("REPLACE_JOB_ROOT", self.name)
        cxml = cxml.replace("REPLACE_HERITRIX_JOBS", settings.HERITRIX_JOBS)
        self.cxml = cxml


    def setup_job_directory(self):
        """Creates the Heritrix job directory."""
        self.job_dir = "%s/%s/" % (settings.HERITRIX_JOBS, self.name)
        if not os.path.isdir(self.job_dir):
            os.makedirs(self.job_dir)

        shutil.copy(settings.HERITRIX_SHORTENERS, self.job_dir)
        shutil.copy(settings.HERITRIX_EXCLUDE, self.job_dir)
        shutil.copy(settings.HERITRIX_SURTS, self.job_dir)

        # Write seeds to disk:
        with open("%s/seeds.txt" % self.job_dir, "wb") as o:
            o.write("\n".join(self.seeds))

        # Write profile to disk:
        self.create_profile()
        with open("%s/crawler-beans.cxml" % self.job_dir, "wb") as o:
            o.write(self.cxml)

        # Write Sheet-associations to disk:
        commands = []
        for target in self.info:
            commands = get_depth_scripts(target["seeds"], target["field_depth"])
            commands += get_scope_scripts(target["seeds"], target["field_scope"])
        commands += get_blocking_scripts()
        with open("%s/script.beanshell" % self.job_dir, "wb") as o:
            o.write("\n".join(commands))


    def run_job_script(self):
        """Runs the 'script.beanshell' located in the job directory."""
        with open("%s/script.beanshell" % self.job_dir, "rb") as i:
            self.heritrix.execute(job=self.name, script=i.read(), engine="beanshell")


    def write_act_info(self):
        """Writes w3act job information to disk."""
        with open("%s/latest/w3act-info.json" % self.job_dir, "wb") as o:
            o.write(json.dumps(self.info, indent=4))


    def waitfor(self, status):
        """Waits for the job to reach a particular status."""
        try:
            while self.heritrix.status(self.name) not in status:
                time.sleep(10)
        except ParseError:
            time.sleep(10)


    def start(self):
        """Starts the job."""
        logger.info("Building %s" % self.name)
        self.heritrix.build(self.name)
        self.waitfor("NASCENT")
        logger.info("Launching %s" % self.name)
        self.heritrix.launch(self.name)
        self.waitfor("PAUSED")
        self.write_act_info()
        logger.info("Running scripts for %s" % self.name)
        self.run_job_script()
        logger.info("Unpausing %s" % self.name)
        self.heritrix.unpause(self.name)
        self.waitfor("RUNNING")


    def stop(self):
        """Stops the job if already running, then starts."""
        if self.heritrix.status(self.name) != "":
            logger.info("Killing alread-running job: %s" % self.name)
            self.heritrix.pause(self.name)
            self.waitfor("PAUSED")
            self.heritrix.terminate(self.name)
            self.waitfor("FINISHED")
            self.heritrix.teardown(self.name)
            self.waitfor("")


    def restart(self):
        self.stop()
        self.start()

€ý5:q
