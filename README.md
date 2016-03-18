Shepherd
========

Coordinates the services that make up the UK Web Archive.

h3cc - Heritrix3 Crawl Controller
=================================

Script to interact with Heritrix directly, to perform some general crawler operations.

The ```info-xml``` command downloads the raw XML version of the job information page, which can the be filtered by other tools to extract information. For example, the Java heap status can be queried like this:

    $ python agents/h3cc.py info-xml | xmlstarlet sel -t -v "//heapReport/usedBytes"

Similarly, the number of novel URLs stored in the WARCs can be determined from:

    $ python agents/h3cc.py info-xml | xmlstarlet sel -t -v "//warcNovelUrls"

You can query the frontier too. To see the URL queue for a given host, use a query-url corresponding to that host, e.g.

    $ python agents/h3cc.py -H 192.168.99.100 -q "http://www.bbc.co.uk/" -l 5 pending-urls-from

This will show the first five URLs that are queued to be crawled next on that host. Similarly, you can ask for information about a specific URL:

    $ python agents/h3cc.py -H 192.168.99.100 -q "http://www.bbc.co.uk/news" url-status


w3act.py - W3ACT Command-Line Client
====================================

This simple CLI client for [W3ACT](https://github.com/ukwa/w3act) uses it's API to extract and submit information. It does not cover every aspect of W3ACT, and is mainly intended for helping with testing the system during development.

This submits a new Target to the system, with one URL, a Title, and a crawl frequency:

    $ python w3act.py add-target "https://www.gov.uk/government/publications?departments[]=department-for-transport" "Department for Transport publications"

The script is designed to log in using the default developer account and credentials. This can be overridden if required, like so:

    $ python w3act.py -u "bob@jam.com" -p "secretsquirrel" add-target "https://www.gov.uk/government/publications?departments[]=department-for-transport" "Department for Transport publications" daily

If all goes well, you'll get back something like:

    [2016-02-03 15:59:44,204] INFO: Logging into http://localhost:9000/act/login as wa-sysadm@bl.uk 
    [2016-02-03 15:59:44,204] INFO w3act.py.__init__: Logging into http://localhost:9000/act/login as wa-sysadm@bl.uk 
    [2016-02-03 15:59:44,219] INFO: Starting new HTTP connection (1): localhost
    [2016-02-03 15:59:44,219] INFO connectionpool.py._new_conn: Starting new HTTP connection (1): localhost
    [2016-02-03 15:59:44,241] INFO: POST {"field_depth": "CAPPED", "field_urls": ["https://www.gov.uk/government/publications?departments[]=department-for-transport"], "title": "Department for Transport publications", "selector": 1, "field_crawl_frequency": "daily", "field_scope": "root", "field_ignore_robots_txt": false, "field_crawl_start_date": 1454515184.0}
    [2016-02-03 15:59:44,241] INFO w3act.py.post_target: POST {"field_depth": "CAPPED", "field_urls": ["https://www.gov.uk/government/publications?departments[]=department-for-transport"], "title": "Department for Transport publications", "selector": 1, "field_crawl_frequency": "daily", "field_scope": "root", "field_ignore_robots_txt": false, "field_crawl_start_date": 1454515184.0}
    [2016-02-03 15:59:44,242] INFO: Starting new HTTP connection (1): localhost
    [2016-02-03 15:59:44,242] INFO connectionpool.py._new_conn: Starting new HTTP connection (1): localhost
    201
    http://localhost:9000/act/targets/1

...where the last line indicates the URL for the newly created Target.

You can set the crawl schedule for a Target like this:

    $ python w3act.py update-target-schedule 1 daily "2016-01-13 09:00:00"

You can turn make an existing Target be a Watched Target (for document harvesting) using:

    $ python w3act.py watch-target 1

...After which, you can add an example document like this:

    $ python w3act.py add-document 1 20160202235322 "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/492092/supplementary-guidance-january-2016.pdf" "https://www.gov.uk/government/publications/department-for-transport-delivers-more-grant-funding-to-transport-freight-by-rail"

The Wayback timestamp is required, along with the document and 'landing page' URLs.

Other Utilities
===============

### mq_peek.py ###

Useful for peeking at messages on a RabbitMQ queue.


Workflow Overview
=================

In order to improve the quality and automation of our crawl workflows, we are shifting to a more modular, queue-based architecture rather than trying to bake all the functionality we need into a single monolithic package. Specifically, we are chaining sequences of small operations together, and using queues as buffers in between them so each component can be managed and scaled separately. This architecture also makes it easier to reuse off-the-shelf components, and makes monitoring the state of the system much easier.

Here's an overview of our current workflow. The light-blue boxes are the queues (provided by a RabbitMQ), and the light-green boxes are the simple Python processes (managed by the supervisord daemon) that pick up the messages and pass the results of their operations on. The darker blue boxes are the other long-running components or applications that manage the data and state information. 

![Workflow Overview](./doc/img/crawl-agents.png)

For example, the main crawl workflow is initated by a ```cron``` task that checks if any crawls are dues to be launched. If so, the script posts a message to a queue of seeds to be processed, and sends a copy of that message to a different queue so we can check later on if it worked okay. A dedicated process (current called the HARchiver) then consumes those messages, uses a RESTful microservice to render the URLs in a web browser (PhantomJS), and then passes the URLs it discovers over to the next queue.

The main crawler, Heritrix3, consumes the list of discovered URLs and queues them internally, with the prioritisation set so as to minimise the delay between the initial launch and the actual data capture.

As each resource is captured, Heritrix3 posts a message to another queue, where they are read by another simple process that POSTs them to a CDX server called tinycdxserver. Once in tinycdxserver, the URLs we have been crawling can be discovered via OpenWayback, only a few seconds after they have been crawled. The OpenWayback service itself knows where the WARCs are stored, and is configured to look in the WARCs currently being written as well as the older content, meaning successfully crawled content can be browsed *in real time*.

In this way, we have a constant stream of archived content -- a continous crawl with live updates.

Generally, the processes in the chain are designed to be stateless, and to only acknowledge the messages they consume when the whole operation has succeeded. This means if something goes wrong at any point in the chain, the system will cope by simply backing-up unprocessed messages on the queue preceding the damaged component.

Crawl Workflow
==============

launcher.py
-----------

* Started by CRON on the hour.
* Reads from W3ACT.
* Launches and crawls due to start that hour.
* Does so using the ```uris-to-render``` queue

### inject.py ###

This utility allows URIs to be launched directly, either into ```uris-to-render``` or ```uris-to-crawl```.

harchiverd.py
-------------

Screenshots the ```uris-to-render```, writes the rendered form to WARCs, and passes the URIs on to ```uris-to-crawl```.

Heritrix3 AMQPUrlReceiver
-------------------------

This class consumes the uris-to-crawl and queues them inside H3.

Heritrix3 AMQPIndexableCrawlLogFeed
-----------------------------------

This disposition-chain processor sends the result of crawling each URI to the ```uris-to-index``` queue. 

Another instance sends ones that appear to be documents to the ```documents-to-catalogue``` queue.

uristocdxserver.py
------------------

This pulls the ```uris-to-crawl``` and POSTs relevant URIs to the ```tinycdxserver``` for QA Wayback.

docstow3act.py
--------------

This pulls the ```documents-to-catalogue``` and checks the resources are available in QA Wayback. It then examines the URLs to extract document-level metadata, and attempts to ensure documents are associated with the right targets. The documents are then POSTed to W3ACT for cataloguing.

    $ python agents/docstow3act.py --amqp-url "amqp://guest:guest@192.168.99.100:5672/%2f" post-crawl DH-1-documents-to-catalogue

Post-Crawl Ingest Workflow
==========================

Original script was:

    $ python agents/sipstodls.py --amqp-url "amqp://guest:guest@192.168.99.100:5672/%2f"

Heritrix 3 AMQPCheckpointSuccessMessageProducer
-----------------------------------------------

After every successful checkpoint operation, this module makes Heritrix emit a message like this:

~~~ json
{
    "checkpointDirAbsolutePath": "/jobs/frequent/checkpoints/cp00001-20160229142814",
    "checkpointDirPath": "cp00001-20160229142814",
    "name": "cp00001-20160229142814",
    "shortName": "cp00001"
}
~~~

## package-checkpoints.py ##

Takes a checkpoint and bundles up all the files and data we need for a submission.

- Extracts the checkpoint ID (e.g. ```cp00001-20160229142814```) from the message and locates the crawl files.
- Parses the crawl log to determine the start date and identify WARCs that make up this checkpoint, including any viral WARCs.
- Mint ARKs for the WARCs and one for the ZIP file, store them in a warc-to-arks.txt file.
- Generate hash manifest for all files, using the SHA-512 algorithm.
- Transfer WARCs to HDFS.
- Create warc-to-hdfs.txt file that maps WARC filenames to HDFS paths.
- ZIP up all crawl files (crawler-beans.cxml, the crawl log and other log files, manifest.sha512, warc-arks.txt,warc-hdfs-paths.txt) except WARCs
- Also hash the ZIP file file
- Transfer ZIP to HDFS and verify the transfer.
- Update WARC-to-HDFS mapping file.
- Update WARC-to-ARK mapping file.
- Update WARC-to-location file(s) (for QA Wayback)
- Delete WARCs/ZIPs from local storage.
- **PASS** a message in the following 'CIPD' (Crawl Information Package Description) format to the ```package-to-sip``` queue.


~~~ json
{
    "startDate": "2016-02-29T12:00:00Z",
    "crawlStream": "frequent",
    "packageId": "frequent-cp00001-20160229142814",
    "warcs": [
        "BL-20160224194138561-00000-44~04917ac61543~8443.warc.gz",
        "..."
    ],
    "viral": [
        "BL-20160224194138561-00000-44~04917ac61543~8443.warc.gz",
        "..."
    ],
    "logs": [
        "frequent-cp00001-20160229142814.zip"
    ],
    "arks": {
        "BL-20160224194138561-00000-44~04917ac61543~8443.warc.gz": "ark:/81055/vdc_100022535899.0x",
        "..."
    },
    "hdfs": {
        "BL-20160224194138561-00000-44~04917ac61543~8443.warc.gz": "/heritrix/output/warcs/frequent/BL-20160224194138561-00000-44~04917ac61543~8443.warc.gz",
        "..."
    }
}
~~~

## generate-sips.py ##

Now we need to wrap the content up for ingest into the DLS.

- Extract the necessary information from the CIPD message
- Generate SIP, by:
    - Generating the METS file.
    - Building a BagIt arounding it.
    - Putting that into a tar.gz file.
- Copy SIP to HDFS.
- Verify transfer to HDFS.
- ***PASS*** a sutiable message to the ```sips-to-submit``` queue.

The format is the same as received, except now with an added ```sip``` field indicating the location of the SIP on HDFS.

~~~ json
        "...",
        "sip": "/heritrix/sips/..."
    }
~~~

## submit-sips.py ##

- Extract HDFS path to SIP from CIPD message
- Download.
- Unpack (TBC?)
- Verify BagIt bag (TBC?)
- Submit to DLS.
- PASS the message on to ```generate-cdx-for-sip```

## generate-cdx.py ##

- Read the list of WARCs from the CIPD message
- Generate CDX for all WARCs in a checkpoint.
- Update cdx-to-merge and warc-to-location files on HDFS
- Merge QA Wayback CDX (TBC?)
- PASS the message on to ```submission-to-verify``` queue.

## verify-sips.py ##

- Extract list of WARCs and their ARKs from the CIPD message.
- Check status of all WARCs according to the DLS report.
- Log the result.
- For Boston Spa, verify all WARCs are available from DLS and that the hashes are the same. (TBC?)
- Once all WARCs are present at a given DLS node:
    - **PUT** a file on HDFS indicating that there is new content to be made available.
    - **PASS** message on to ```check-updated-access-cdx-for-sip``` with an extra field to indicate which node is ready.

## merge-cdx.py (Runs on every access node) ##

- **CRON** on delivery node looks for new cdxs-to-merge info on HDFS for this node.
- Updates local WARC-to-location file.
- Updates local CDX file.
- **PUTS** cdx-merged file on HDFS.

## check-cdx-merged.py ##

- Extract identity of the cdx-to-check from the CIPD message from the ```check-updated-access-cdx-for-sip``` queue.
- Look for cdx-merged notification on HDFS.
- Remove the temporary files from HDFS.
- DONE


Post-Crawl QA Workflow
======================

checkforuris.py
---------------


Others (TBA)
------------
Note that the other aspects, like depth etc, and setup periodically via "h3cc fc-sync".
Separate process bundles up per checkpoint (gather.py)	
Separate process sends Documents to a queue (in H3) and sends the queue to W3ACT (mule.py)
muster.py, yoke.py, shear.py, rouseabout, riggwelter (upside down sheep), 
lanolin (grease), cull.py, heft (land), flock, fold, dip, bellwether (flock lead)

