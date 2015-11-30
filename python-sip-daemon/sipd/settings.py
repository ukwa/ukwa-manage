SIP_QUEUE_NAME = "sips"
SIP_QUEUE_KEY = "sips"
SIP_QUEUE_HOST = "localhost"

SUBMITTED_QUEUE_NAME = "sip-submitted"
SUBMITTED_QUEUE_KEY = "sip-submitted"
SUBMITTED_QUEUE_HOST = "localhost"

ERROR_QUEUE_HOST = "localhost"
ERROR_QUEUE_NAME = "sip-error"
ERROR_QUEUE_KEY = "sip-error"

PID_FILE = "/var/run/sipd/sipd.pid"
LOG_FILE = "/var/log/sipd/sipd.log"

SIP_ROOT = "/heritrix/sips"
DLS_ROOT = "%s/dls-mount" % SIP_ROOT
DLS_DROP = "%s/Dropfolder" % DLS_ROOT
DLS_WATCH = "%s/WatchFolder" % DLS_ROOT

WEBHDFS_PREFIX = "http://dls.httpfs.wa.bl.uk:14000/webhdfs/v1"
WEBHDFS_USER = "heritrix"

DUMMY = False

