Tasks
=====

Short term, we need to automate document extraction and crawl report generation



Crawler Tasks
-------------

| Name | Description |
|------|-------------|

Current inside Docker we run:

- Pulse start/stop (Currently hard-hooked into W3ACT feeds - maybe cache those feeds on HDFS and use that from crawler engine)
- Moving WREN files into the job launch folder.
- ScanForFilesToMove (i.e. move-to-HDFS but no delete right now)

We should also consider running:

- Closing open WARCs and removing .lck files for old/dead jobs.
- Scan remote and upload (manually run but not production)
- Scan Crawler03 and make sure we have the old SIPs.
- Update GeoIP databases
- Make move-to-hdfs work for DC
- FTP of Nominet data to HDFS
- The task that gets log files and pushes them up to HDFS (hdfssync).


Processing Tasks
----------------


But soon, e.g. before we run our of disk space on crawler01, we need:

- ScanRemoteAndUpload
- Assemble
- DeleteRemote

To submit to DLS we need

- Package
- Generate incremental packages for all crawls.
- Submit


- GenerateCrawlLogReports (which extracts documents and is not automated)
- Generate summary statistics from crawl logs
- Extract basic WARC metadata and generate graphs etc.


Discovery Tasks
------------

| Name | Description |
|------|-------------|
| Test | tester |


As the website becomes stable, we will need

- Update OUKWA Whitelist
- Update Index Annotations
- Update Collections Solr


Monitoring Tasks
----------------

Note results are generated from state independently, using the tasks in the [ukwa-monitor](https://github.com/ukwa/ukwa-monitor) codebase.

- HDFS Monitoring



Backup Tasks
------------

| Name | Description |
|------|-------------|
| [BackupRemoteDockerPostgres](../shepherd/tasks/backup/postgresql.py) | Connects to a remote PostgreSQL instance running in a Docker container and takes a backup.|


