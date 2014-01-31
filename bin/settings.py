DEFAULT_HOUR = 12
DEFAULT_DAY = 8
DEFAULT_WEEKDAY = 0
DEFAULT_MONTH = 1
DEFAULT_WAIT = 10

CONFIG_ROOT = "/heritrix/git/heritrix_bl_configs"
HERITRIX_PROFILES = CONFIG_ROOT + "/profiles"
HERITRIX_EXCLUDE = CONFIG_ROOT + "/exclude.txt"
HERITRIX_SHORTENERS = CONFIG_ROOT + "/url.shorteners.txt"
HERITRIX_SURTS = CONFIG_ROOT + "/surts.txt"
HERITRIX_JOBS = "/opt/heritrix/jobs"
WARC_ROOT = "/heritrix/output/warcs"
LOG_ROOT = "/heritrix/output/logs"
URL_ROOT = "http://www.webarchive.org.uk/act/websites/export/"

QA_CDX = "/heritrix/output/wayback/cdx-index/qa.cdx"
CDX = "/heritrix/output/wayback/cdx-index/index.cdx"
PATH_INDEX = "/heritrix/output/wayback/qa-path-index.txt"
WAYBACK = "http://opera.bl.uk:9090/qa/"
WAYBACK_LOG = "/opt/qa-wayback/logs/catalina.out"
JOB_ROOT = "/opt/heritrix/jobs/"
PHANTOMJS = "/opt/phantomjs/bin/phantomjs"
NETSNIFF = "/opt/phantomjs/examples/netsniff.js"
TIMEOUT = 60 * 20
