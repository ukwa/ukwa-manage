LOCAL_ROOT = "/heritrix/output"
WARC_ROOT = "%s/warcs" % LOCAL_ROOT
LOG_ROOT = "%s/logs" % LOCAL_ROOT
VIRAL_ROOT = "%s/viral" % LOCAL_ROOT
ARK_URL="http://pii.ad.bl.uk/pii/vdc?arks="
ARK_PREFIX="ark:/81055/vdc_100022535899.0x"
JOBS_ROOT="/opt/heritrix/jobs"

BAGIT_CONTACT_NAME="Roger G. Coram"
BAGIT_CONTACT_EMAIL="roger.coram@bl.uk"
BAGIT_DESCRIPTION="LD Crawl: "

ZIP="/opt/zip30/bin/zip -jg"
UNZIP="/opt/unzip60/bin/unzip"

OUTPUT_ROOT="/heritrix/sips"

WEBHDFS_PREFIX="http://dls.httpfs.wa.bl.uk:14000/webhdfs/v1"
WEBHDFS_SUFFIX="?user.name=hadoop&op=OPEN"
WEBHDFS_USER="heritrix"

PERMISSION_STATE="Granted"
PERMISSION_START_DATE="2013-04-06"
PERMISSION_NAME="The Legal Deposit Libraries (Non-Print Works) Regulations 2013"
PERMISSION_PUBLISHED="True"
HERITRIX = "heritrix-3.1.1+uk.bl.wap"

CLAMD_CONF = "/opt/heritrix/clamd/3310.conf"
NUM_THREADS=10
