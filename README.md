Shepherd
========

Coordinates the services that make up the UK Web Archive.



# Note that the other aspects, like depth etc, and setup periodically via "h3cc fc-sync".
# Separate process bundles up per checkpoint (gather.py)	
# Separate process sends Documents to a queue (in H3) and sends the queue to W3ACT (mule.py)
# muster.py, yoke.py, shear.py, rouseabout, riggwelter (upside down sheep), 
# lanolin (grease), cull.py, heft (land), flock, fold, dip, bellwether (flock lead)


    $ python agents/sipstodls.py --amqp-url "amqp://guest:guest@192.168.99.100:5672/%2f"

    $ python agents/docstow3act.py --amqp-url "amqp://guest:guest@192.168.99.100:5672/%2f" post-crawl DH-1-documents-to-catalogue


h3cc - Heritrix3 Crawl Controller
---------------------------------

Script to interact with Heritrix directly, to perform some general crawler operations.

The ```info-xml``` command downloads the raw XML version of the job information page, which can the be filtered by other tools to extract information. For example, the Java heap status can be queried like this:

    $ python agents/h3cc.py info-xml | xmlstarlet sel -t -v "//heapReport/usedBytes"

Similarly, the number of novel URLs stored in the WARCs can be determined from:

    $ python agents/h3cc.py info-xml | xmlstarlet sel -t -v "//warcNovelUrls"
