# ukwa-tasks
Luigi tasks for running Hadoop jobs and managing material held on HDFS

## Getting started

n.b. we currently run Python 2.7, although code should be compatible.

### Set up the environment

    sudo pip install virtualenv
    virtualenv -p python2.7 venv
    source venv/bin/activate
    pip install -r requirements.txt


## Overall Workflow

The main process of driving content management, based on what's on HDFS, starts with the tasks in
`tasks/hdfs/listings.py`. This generates various summaries and reports of the content of HDFS that other tasks use
to updated downstream systems.

Each 'listing' task also generated summary metrics that are pushed to Prometheus. These can be used to set up alerts
so that we will notice the tasks are not being run, or if the metrics are not trending as we expect.


### Listing WARCs by date

The HDFS listing process generates a summary of every WARC file, grouped by the day the file was created. As subsequent
scans can add more files (due to the time it can take to copy content up from the crawlers), we need to make sure the
file manifests are update in a way that ensures this gets noticed. We do this by including the number of files in the
file name itself.

The general naming scheme is:

    /var/state/warcs/<YYYY>-<MM>/<YYYY>-<MM>-<DD>-warcs-<NNNN>-warc-files-for-date.txt

e.g.

    /var/state/warcs/2018-02/2018-02-20-warcs-57-warc-files-for-date.txt

Which lists the 57 WARCs known to be associated with that date at the time the list was created. If subsequent runs
discover new WARCs, new files will be created...

    /var/state/warcs/2018-02/2018-02-20-warcs-68-warc-files-for-date.txt

n.b. using just the file number is simple, but assumes WARCs will not be removed from the cluster. It may be
possible to avoid this by using e.g. the truncated hash of all the warc file names

### CDX Indexing (with verification)

The downstream processing treats each of the above inventory files as ExternalFiles. For every date, it attempts to
ensure that every file set is processed, and that the presence of each WARC file's content in the CDX index is verified.

### Solr Indexing (with verification)



### Hashing and ARK minting

Hashes, possibly lodged by move-to-hdfs?

### Pre-packaging preparation
