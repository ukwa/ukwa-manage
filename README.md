python-hdfssync
===============

Scripts for IO between local filsystem and HDFS.

hdfslogsync.py
-----------

Currently this is configured to store all logs in a subdirectory of hdfs:///logs/,
named after the local hostname. Logs are stored under this location, mirroring
their local location, e.g.:

    /logs/opera.bl.uk/opt/tomcat/logs/catalina.out

NB: This requires that the subdirectory already exists: the script will not 
attempt to create it!

hdfscdxsync.py
----------

This will check whether the CDX stored in a HDFS directory is larger than that
at a configured local location. If so, it will replace the local version.

Prerequisites & Installation
----------------------------

This depends on the python-webhdfs package:

    git clone https://github.com/PsypherPunk/python-webhdfs.git
    pip install ./python-webhdfs/

Once this is installed, installation follows in a similar fashion:

    git clone gitlab@git.wa.bl.uk:/repos/products/python-hdfssync.git

At this point you can edit the python-hdfssync/hdfssync/settings.py file to
include additional directories; paths are stored as a comma-separated list:

    logdirectories="/opt/tomcat/logs,/var/log/httpd"

Once updated, install via Pip:

    pip install ./python-hdfssync/

