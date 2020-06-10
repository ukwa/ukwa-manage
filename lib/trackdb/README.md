TrackDB Library & Command
=========================

The TrackDB library provides a standard helper class to manage a Tracking Database, based on using an Apache Solr collection as the back-end storage.

The library can be used from a task framework like Luigi, and there ia also a `trackdb` command-line tool that provides a way to interact with the Tracking Database for testing or ad-hoc work.

The `trackdb` command
---------------------

For example, to control the process of CDX indexing, we need to know where the WARCs are, and have a convention for recording which ones have been indexed into which CDX services.  We do this by populating the Tracking Database with records for every file, and classifying the relevant `warc.gz` files as `kind_s:warcs` so they can be picked out. The `store` library is used to list the contents of the store, and then `trackdb` is used to classify the files and record them in the Tracking Database.

First, we list the files, classify them into warcs/logs/etc. and generate a big batch of metadata in line-separated JSON format...

    store list --recursive / > hdfs-file-listing.jsonl

But as that uses WebHDFS, listing _all files_ is rather slow. Alternatively, a standard Hadoop recursive file listing can be used and post-processed to get the same result:

    hadoop fs -lsr / > hdfs-file-listing.lsr
    store lsr-to-jsonl hdfs-file-listing.lsr hdfs-file-listing.jsonl

Once we have that, we can import them into the TrackDB:

    trackdb import files hdfs-file-listing.jsonl

We can query the TrackDB to see what we have. Some common queries and reports are built into the `trackdb` tool.

Once populated, the TrackDB is used to drive things like indexing processes, via the [`windex` command](../windex/README.md).