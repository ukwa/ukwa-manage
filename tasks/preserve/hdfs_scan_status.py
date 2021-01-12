#!/usr/bin/env python
# encoding: utf-8
"""
This module performs preservation-related HDFS tasks
"""

import luigi
from lxml import html
import requests
import json
import datetime
from tasks.common import state_file
from prometheus_client import CollectorRegistry, Gauge


class GatherBlockScanReports(luigi.Task):
  """
  Gathers the block scan reports from all the HDFS nodes:
  """
  task_namespace = 'preserve'
  date = luigi.DateParameter(default=datetime.date.today())

  results = {}
  keys = {}

  def output(self):
    return state_file(self.date,'hdfs', 'block-scanner-reports.json')


  def run(self):

    page = requests.get('http://namenode.api.wa.bl.uk/dfsnodelist.jsp?whatNodes=LIVE')
    tree = html.fromstring(page.content)

    grunts = tree.xpath('//td[@class="name"]/a/text()')


    for grunt in grunts:
      self.results[grunt] = {}
      rep = requests.get('http://%s:50075/blockScannerReport' % grunt )
      for line in rep.iter_lines(decode_unicode=True):
        if ":" in line:
          # Split into key-value and clean up:
          key, val = line.split(":")
          key = key.lower().strip().replace(' ','_')
          if val.endswith('%'):
            val = float(val.rstrip('%'))/100.0
            key = key + '_percent'
          elif not key.endswith('_kbps'):
            key = key + '_count'
          # And collect:
          self.keys[key] = line.strip()
          self.results[grunt][key] = val

    with self.output().open('w') as f:
      f.write(json.dumps(self.results, indent=2))

  def get_metrics(self,registry):
    # type: (CollectorRegistry) -> None

    for key in self.keys:
      g = Gauge('hdfs_datanode_%s' % key, self.keys[key],
              labelnames=['datanode'], registry=registry)
      for grunt in self.results:
        g.labels(datanode=grunt).set(self.results[grunt][key])



if __name__ == '__main__':
    # Running from Python, but using the Luigi scheduler:
    luigi.run(['preserve.GatherBlockScanReports'])

