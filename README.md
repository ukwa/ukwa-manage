# UKWA Manage
Tools for managing the UK Web Archive

## Dependencies

This codebase contains many of the command-line tools used to run automation tasks at the UK Web Archive, via the Docker container version, orchestrated by Apache Airflow.  This runs local command and [MrJob](https://mrjob.readthedocs.io/) Hadoop jobs, with the latter coded in either Java or Python, able to run on the older or newer Hadoop clusters.  As such, the dependencies are quite complex.

- The base Docker image is from https://github.com/ukwa/docker-hadoop and supports two versions of Hadoop and Python 3.
- The https://github.com/ukwa/webarchive-discovery Docker image is used as the source for JARs for Java Hadoop jobs.
- This project also depends on four UKWA modules: [`hapy-heritrix`](https://github.com/ukwa/hapy), [`python-w3act`](https://github.com/ukwa/python-w3act), [`crawlstreams`](https://github.com/ukwa/crawl-streams) directly and [`kevals`](https://github.com/ukwa/kevals) indirectly (via `crawlstreams`).

Requirements in builds are handled via `requirements.txt` files, which pin specific versions of dependencies.  Therefore, for the builds to remain consistent, the versions of modules that appear in multiple places (e.g. `requests`) have to be synchronised across all placed. For example, upgrading `requests` means updating all five Python codebases, otherwise the build will fail.

Similarly, if upgrading the version of Python, to ensure full compatibility, this needs to be done across all dependencies including the Docker base image (as well as all nodes in the Hadoop cluster, with the `mrjob_*.conf` files updated to refer to it).

## Getting started

n.b. we currently run Python 3.7 on the Hadoop cluster, so streaming
Hadoop tasks need to stick to that version.

### Set up a Python 3.7 environment

```
  sudo yum install snappy-devel
  sudo pip install virtualenv
  virtualenv -p python3.7 venv
  source venv/bin/activate
```

Install UKWA modules and other required dependencies:

```
  pip install --no-cache --upgrade https://github.com/ukwa/hapy/archive/master.zip
  pip install --no-cache --upgrade https://github.com/ukwa/python-w3act/archive/master.zip
  pip install --no-cache --upgrade https://github.com/ukwa/crawl-streams/archive/master.zip
  pip install -r requirements.txt
```

### Running the tools

To run the tools during development:

```
  export PYTHONPATH=.
  python lib/store/cmd.py -h
```

To install:

```
  python setup.py install
```

then e.g.

```
  store -h
```

Or they can be built and run via Docker, which is useful for runs that need to run Hadoop jobs, and for rolling out to production. e.g.

```
docker-compose build tasks
docker-compose run tasks store -h
```

## Management Commands:

The main management commands are `trackdb`, `store` and `windex`:

### `trackdb`

This tool is for directly working with the TrackDB, which we use to keep track of what's going on. See <lib/trackdb/README.md> for details.

### `store`

This tool is for working with the HDFS store via the WebHDFS API, e.g uploading and downloading files. See <lib/store/README.md> for details.


### `windex`

This tool is for managing our CDX and Solr indexes - e.g. running indexing jobs. It talks to the TrackDB, and can also talk to the HDFS store if needed. See <lib/windex/README.md> for details.

## Code and configuration

The older versions of this codebase are in the `prototype` folder, so we can copy in and update tasks as we need.  The tools are defined in sub-folders of the `lib` folder, and some Luigi tasks are defined in the `tasks` folder.

A Luigi configuration file is not currently included, as we have to use two different files to provides two different levels of integration. In short, `ingest` services are given write access to HDFS via the Hadoop command line, while `access` services have limited read-only access via our proxied WebHDFS gateway.


## Example: Manually Processing a WARC collection

_This probably needs to be simplified and moved to a separate page_


We collected some WARCs for EThOS as an experiment. 

A script like this was used to upload them:

```
#!/bin/bash
for WARC in warcs/*
do
  docker run -i -v /mnt/lr10/warcprox/warcs:/warcs ukwa/ukwa-manage store put ${WARC} /1_data/ethos/${WARC}
done
```

Note that we're using the Docker image to run the tasks, to avoid having to install the software on the host machine.

The files can now be listed using:

```
docker run -i ukwa/ukwa-manage store list -I /1_data/ethos/warcs > ethos-warcs.ids
docker run -i ukwa/ukwa-manage store list -j /1_data/ethos/warcs > ethos-warcs.jsonl
```

The JSONL format can be imported into TrackDB (defaults to used the DEV TrackDB).

```
cat ethos-warcs.jsonl | docker run -i ukwa/ukwa-manage trackdb files import -
```

These can then be manipulated to set them up as a kind of content stream:

```
cat ethos-warcs.ids | trackdb files update --set stream_s ethos -
cat ethos-warcs.ids | trackdb files update --set kind_s warcs -
```

......


## Heritrix Jargon


* [Queue States](https://webarchive.jira.com/wiki/spaces/Heritrix/pages/5735753/Glossary#Glossary-QueueStates)

### Notes on queue precedence

A queue's precedence is determined by the precedence provider, usually based on the last crawled URI. Note that a lower precedence value means 'higher priority'.

Precedence is used to determine which queues are brought from inactive to active first. Once the precedence of a queue exceeds the 'floor' (255 by default), it is considered ineligible and won't be crawled any further.

The vernicular here is confusing. Floor is in reference to the least priority but is actually the highest allowed integer value.

In practice, unless you use a special precedence policy or tinker with the precedence floor, you will never hit an ineligible condition.

A use for this would be a precedence policy that gradually lowers the precedence (cumulatively) as it encounters more and more 'junky' URLs. But I'm not aware of anyone using it in that manner.


