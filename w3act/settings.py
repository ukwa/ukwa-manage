LOG_ROOT="/var/log/w3act"
PID_ROOT="/var/run/w3actd"

JOB_ERROR_QUEUE_HOST="localhost"
JOB_ERROR_QUEUE_NAME="w3act-error"
JOB_ERROR_QUEUE_KEY="w3act-error"

JOB_QUEUE_HOST="localhost"
JOB_QUEUE_NAME="w3actqueue"

W3ACT_BASE="https://www.webarchive.org.uk/act-ddhapt"
W3ACT_LOGIN="%s/login" % W3ACT_BASE
W3ACT_EMAIL="wa-sysadm@bl.uk"
W3ACT_PASSWORD="sysAdmin"
W3ACT_EXPORT_BASE="%s/targets/export" % W3ACT_BASE
W3ACT_LD_EXPORT="%s/ld" % W3ACT_EXPORT_BASE
W3ACT_JOB_FIELD="url"

HERITRIX_ROOT="/opt/heritrix"
HERITRIX_JOBS="%s/jobs" % HERITRIX_ROOT
HERITRIX_CONFIG_ROOT="/heritrix/git/heritrix_bl_configs"
HERITRIX_PROFILE="%s/profiles/profile-frequent.cxml" % HERITRIX_CONFIG_ROOT
HERITRIX_EXCLUDE="%s/exclude.txt" % HERITRIX_CONFIG_ROOT
HERITRIX_SHORTENERS="%s/url.shorteners.txt" % HERITRIX_CONFIG_ROOT
HERITRIX_SURTS="%s/surts.txt" % HERITRIX_CONFIG_ROOT

CLAMD_PORT="3310"

