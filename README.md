# UKWA Manage
Luigi tasks for managing the UK Web Archive

## Getting started

n.b. we currently run Python 2.7 on the Hadoop cluster, so streaming
Hadoop tasks need to stick to that version. Other code should be written
in Python 3 but be compatible with both where possible.

### Set up a Python 2.7 environment

    sudo pip install virtualenv
    virtualenv -p python2.7 venv
    source venv/bin/activate
    pip install -r requirements.txt

  942  cd github/
  943  git clone https://github.com/ukwa/ukwa-manage.git
  944  cd ukwa-manage/
  946  pip install virtualenv
  947  virtualenv venv
  948  source venv/bin/activate
       pip install --no-cache https://github.com/ukwa/hapy/archive/master.zip
       pip install --no-cache https://github.com/ukwa/python-w3act/archive/master.zip
  949  pip install -r requirements.txt
  953  yum install snappy-devel
  954  pip install -r requirements.txt
  955  python scripts/crawlstreams.py -k crawler04:9094 -q uris.tocrawl.dc


### Code and configuration

The older versions of this codebase are in the `prototype` folder, so we can copy in and update tasks as we need.  The tasks we currently use are in the `tasks` folder and some shared utility code is in the `lib` folder.

A Luigi configuration file is not currently included, as we have to use two different files to provides two different levels of integration. In short, `ingest` services are given write access to HDFS via the Hadoop command line, while `access` services have limited read-only access via our proxied WebHDFS gateway.


## Heritrix Jargon


* [Queue States](https://webarchive.jira.com/wiki/spaces/Heritrix/pages/5735753/Glossary#Glossary-QueueStates)

### Notes on queue precedence

A queue's precedence is determined by the precedence provider, usually based on the last crawled URI. Note that a lower precedence value means 'higher priority'.

Precedence is used to determine which queues are brought from inactive to active first. Once the precedence of a queue exceeds the 'floor' (255 by default), it is considered ineligible and won't be crawled any further.

The vernicular here is confusing. Floor is in reference to the least priority but is actually the highest allowed integer value.

In practice, unless you use a special precedence policy or tinker with the precedence floor, you will never hit an ineligible condition.

A use for this would be a precedence policy that gradually lowers the precedence (cumulatively) as it encounters more and more 'junky' URLs. But I'm not aware of anyone using it in that manner.


