Shepherd
========

Coordinates the services that make up the UK Web Archive.


h3cc - Heritrix3 Crawl Controller
---------------------------------

Script to interact with Heritrix directly, to perform some general crawler operations.

The ```info-xml``` command downloads the raw XML version of the job information page, which can the be filtered by other tools to extract information. For example, the Java heap status can be queried like this:

    $ python agents/h3cc.py info-xml | xmlstarlet sel -t -v "//heapReport/usedBytes"

Similarly, the number of novel URLs stored in the WARCs can be determined from:

    $ python agents/h3cc.py info-xml | xmlstarlet sel -t -v "//warcNovelUrls"


w3act.py - W3ACT Command-Line Client
------------------------------------

This simple CLI client for [W3ACT](https://github.com/ukwa/w3act) uses it's API to extract and submit information. It does not cover every aspect of W3ACT, and is mainly intended for helping with testing the system during development.

This submits a new Target to the system, with one URL, a Title, and a crawl frequency:

    $ python w3act.py add-target "https://www.gov.uk/government/publications?departments[]=department-for-transport" "Department for Transport publications" daily

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

You can turn make an existing Target be a Watched Target (for document harvesting) using:

    $ python w3act.py watch-target 1

...After which, you can add an example document like this:

    $ python w3act.py add-document 1 20160202235322 "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/492092/supplementary-guidance-january-2016.pdf" "https://www.gov.uk/government/publications/department-for-transport-delivers-more-grant-funding-to-transport-freight-by-rail"

The Wayback timestamp is required, along with the document and 'landing page' URLs.


Others (TBA)
------------

    $ python agents/sipstodls.py --amqp-url "amqp://guest:guest@192.168.99.100:5672/%2f"

    $ python agents/docstow3act.py --amqp-url "amqp://guest:guest@192.168.99.100:5672/%2f" post-crawl DH-1-documents-to-catalogue


Note that the other aspects, like depth etc, and setup periodically via "h3cc fc-sync".
Separate process bundles up per checkpoint (gather.py)	
Separate process sends Documents to a queue (in H3) and sends the queue to W3ACT (mule.py)
muster.py, yoke.py, shear.py, rouseabout, riggwelter (upside down sheep), 
lanolin (grease), cull.py, heft (land), flock, fold, dip, bellwether (flock lead)

