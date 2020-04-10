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

    windex cdx-hadoop-job --cdx-server http://cdx.api.wa.bl.uk/ --collection data-heritrix warcs-to-index.txt

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


TBMoved
=======

We can query the CDX from the command-line:

```
$ windex -C ethos cdx-query http://theses.gla.ac.uk/1158/1/2009ibrahamphd.pdf
uk,ac,gla,theses)/1158/1/2009ibrahamphd.pdf 20200404014648 http://theses.gla.ac.uk/1158/1/2009ibrahamphd.pdf application/pdf 200 FH7MXPURQT7S75IVEUUFWPA2XPOTY3VW - - 7803924 643334769 /1_data/ethos/warcs/WARCPROX-20200404014942362-00230-mja43xl7.warc.gz
```

Now we use the filename, offset and length to grab the WARC record:

```
$ store get --offset 643334769 --length 7803924 /1_data/ethos/warcs/WARCPROX-20200404014942362-00230-mja43xl7.warc.gz temp.warc.gz
```

This gets the WARC record (and oddly, all following ones?!)

```
$ warcio extract --payload temp.warc.gz 0 > file.pdf
```

So now we have the PDF.

TO DO
=====

Priorities:

1. Validating TrackDB with HDFS listing, indexing. Then moving W3ACT.
2. Extracting full-texts from EThOS theses.


- webarchive-discovery:
    - DONE: JSONL files as an alternative output... (Re-purposing MDX code.) 
    - DONE: Extract record range properly.
    - DROID mark problem still present, truncation of file?
    - print exception on closure of the hashcash?
    - Truncation too soon?
    - GC Overhead exception to handle?
    - Disable preflight for this.


2020-04-09 20:33:39 ERROR AbstractPayloadAnalyser:77 - uk.bl.wa.parsers.ApachePreflightParser.parse(): Stream Closed
java.io.IOException: Stream Closed
	at java.io.RandomAccessFile.readBytes(Native Method)
	at java.io.RandomAccessFile.read(RandomAccessFile.java:377)
	at org.jwat.common.RandomAccessFileInputStream.read(RandomAccessFileInputStream.java:105)
	at java.io.BufferedInputStream.fill(BufferedInputStream.java:246)
	at java.io.BufferedInputStream.read1(BufferedInputStream.java:286)
	at java.io.BufferedInputStream.read(BufferedInputStream.java:345)
	at java.io.FilterInputStream.read(FilterInputStream.java:107)
	at org.apache.pdfbox.io.IOUtils.copy(IOUtils.java:66)
	at org.apache.pdfbox.io.RandomAccessBufferedFileInputStream.createTmpFile(RandomAccessBufferedFileInputStream.java:126)
	at org.apache.pdfbox.io.RandomAccessBufferedFileInputStream.<init>(RandomAccessBufferedFileInputStream.java:113)
	at org.apache.pdfbox.preflight.parser.PreflightParser.<init>(PreflightParser.java:164)
	at uk.bl.wa.parsers.ApachePreflightParser.parse(ApachePreflightParser.java:97)
	at uk.bl.wa.analyser.payload.AbstractPayloadAnalyser$ParseRunner.run(AbstractPayloadAnalyser.java:75)
	at java.lang.Thread.run(Thread.java:745)


- Windex:
    - Allow windex to help retrieve record or payload from the Store? Much more convenient.

WHILE Switching to TrackDBTaskTarget for tasks...
- TrackDB:
    - Allow import of HDFS records.
    - Allow more generic {key: value} updates? (Same as above?)
- TASK: Generate HDFS file listings. Import listings to TrackDB: classify, send to Solr as updates.
- TASK: CDX indexing.
- TASK: CDX verification.
- TASK: Solr indexing (leave verification for now?)
- TASK: Backup W3ACT.
- Move ACL, collection solr, indexer annotations and TLR to the python-w3act codebase.
- TASK: Generates all the W3ACT derivatives.
- Store:
    - Add flag to the upload-with-checks method, to handle optional local file deletion.
    - Add threaded uploads to store.put of a folder, test on EThOS.
- Setup warc-server properly, across local crawler files (NFS) and HDFS?
