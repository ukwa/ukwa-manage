TrackDB Library & Command
=========================

The TrackDB library provides a standard helper class to manage a Tracking Database, based on using an Apache Solr collection as the back-end storage.

The library can be used from a task framework like Luigi, and there ia also a `trackdb` command-line tool that provides a way to interact with the Tracking Database for testing or ad-hoc work.

The `trackdb` command
---------------------

For example, to control the process of CDX indexing, we need to know where the WARCs are, and have a convention for recording which ones have been indexed into which CDX services.  We do this by populating the Tracking Database with records for every file, and classifying the relevant `warc.gz` files as `kind_s:warcs` so they can be picked out. The `store` library is used to list the contents of the store, and then `trackdb` is used to classify the files and record them in the Tracking Database.

    store list --recursive / > hdfs-file-listing.txt
    trackdb import files hdfs-file-listing.txt

This processes the file list to classify the items, and imports the resulting records into the Tracking Database.

For CDX indexing, we have chosed to add a multivalued status field to record what we're doing, and called it `cdx_index_ss`. The WARCs on the storage service start off having no value of this field, and this fact is used to identify those that require indexing. After indexing, we set `cdx_index_ss:[COLLECTION_unverified]` to indicate that we've indexed the WARC into a CDX collection called `COLLECTION`, but not checked the index worked (yet). After checking it worked, we will classify the warcs as `cdx_index_ss:[COLLECTION]`.

So, to get a list of WARCs that have not yet been indexed:

    trackdb list warcs --no-field cdx_index_ss > warcs-to-index.txt

These lists can be filtered and limited in various ways (see `trackdb -h` for details):

    trackdb list warcs --stream frequent --year 2020 --no-field cdx_index_ss > warcs-to-index.txt

...this limits the list to files from the `frequent` crawl stream, from the given date range.

These commands can be used to generate a list of WARCs to pass to the CDX indexer Hadoop job:

    cdx hadoop --cdx-server http://cdx.api.wa.bl.uk/ --collection data-heritrix warcs-to-index.txt

After that completes successfully, we can update the status of each individual WARC to be `cdx_index_ss:data-heritrix_unverified`, e.g. 

    trackdb update warcs --add cdx_index_ss data-heritrix_unverified /1_data/test.warc.gz

Or use `-` to indicate a list of HDFS identifiers to be processed in the same way:

    cat warcs-to-index.txt | trackdb warcs update --add cdx_index_ss data-heritrix_unverified -

We can now list items awaiting verification:

    trackdb list warcs --field cdx_index_ss:data-heritrix_unverified > warcs-to-verify.txt

And verify them:

    cdx verify warcs-to-verify.txt TBC

Then when each has been verified, update the record accordingly:

    trackdb update warcs --remove cdx_index_ss data-heritrix_unverified --add cdx_index_ss data-heritrix /1_data/test.warc.gz

Or again as a piped list:

    cat warcs-to-verify.txt | trackdb update warcs --remove cdx_index_ss data-heritrix_unverified --add cdx_index_ss data-heritrix -

The records for all those WARCs now shows `cdx_index_ss:[data-heritrix]`, indicating that the given WARC has been indexed and verified as being present in the `data-heritrix` CDX index service.
