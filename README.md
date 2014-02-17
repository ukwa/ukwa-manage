python-har-daemon
=====================

Daemon intended to monitor a queue to which Heritrix will submit URLs. On
receipt, the URL is submitted to a webservice (currently via
django-phantomjs) and stores the response, a modified HAR record, in a WARC
file.
