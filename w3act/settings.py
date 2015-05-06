LOG_ROOT="/var/log/w3act"
PID_ROOT="/var/run/w3act"

QUEUE_HOST="localhost"
JOB_QUEUE_NAME="w3actqueue"
JOB_ERROR_QUEUE_NAME="w3act-error"
JOB_ERROR_QUEUE_KEY="w3act-error"
SIP_QUEUE_NAME="sips"
SIP_QUEUE_KEY="sips"
QA_QUEUE_NAME="qa"
QA_QUEUE_KEY="qa"

W3ACT_BASE="https://www.webarchive.org.uk/act-ddhapt"
W3ACT_LOGIN="%s/login" % W3ACT_BASE
W3ACT_EMAIL="roger.coram@bl.uk"
W3ACT_PASSWORD="g07*$ECAVDU8QoU"
W3ACT_EXPORT_BASE="%s/targets/export" % W3ACT_BASE
W3ACT_LD_EXPORT="%s/ld" % W3ACT_EXPORT_BASE
W3ACT_JOB_FIELD="url"

HERITRIX_HOST="opera.bl.uk"
HERITRIX_ROOT="/opt/heritrix"
HERITRIX_JOBS="%s/jobs" % HERITRIX_ROOT
HERITRIX_CONFIG_ROOT="/heritrix/git/heritrix_bl_configs"
HERITRIX_PROFILE="%s/profiles/profile-frequent.cxml" % HERITRIX_CONFIG_ROOT
HERITRIX_EXCLUDE="%s/exclude.txt" % HERITRIX_CONFIG_ROOT
HERITRIX_SHORTENERS="%s/url.shorteners.txt" % HERITRIX_CONFIG_ROOT
HERITRIX_SURTS="%s/surts.txt" % HERITRIX_CONFIG_ROOT

JOB_RESTART_HOUR=12
JOB_RESTART_WEEKDAY=1
JOB_RESTART_DAY=1
JOB_RESTART_MONTH=1
W3ACT_FIELDS=["id", "title", "crawlStartDateText", "crawlEndDateText", "field_depth", "field_scope", "field_ignore_robots_txt"]

FREQUENCIES = ["daily", "weekly", "monthly", "quarterly", "sixmonthly", "annual"]
HERITRIX_PORTS = { "daily": "8443", "weekly": "8443", "monthly": "8443", "quarterly": "8443", "sixmonthly": "8443", "annual": "8443" }
CLAMD_PORTS = { "daily": "3310", "weekly": "3310", "monthly": "3310", "quarterly": "3310", "sixmonthly": "3310", "annual": "3310" }
CLAMD_DEFAULT_PORT = "3310"
MAX_RENDER_DEPTH = { "daily": 0, "weekly": 1, "monthly": 1, "quarterly": 1, "sixmonthly": 1, "annual": 1 }

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "simple": {
            "format": "[%(asctime)s] %(levelname)s: %(message)s"
        }
    },
    "handlers": {
        "file": {
            "level": "DEBUG",
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "simple",
            "filename": "%s/%s.log" % (LOG_ROOT, __name__),
            "maxBytes": 10485760
        }
    },
    "loggers": {
        "w3act": {
            "level": "DEBUG",
            "handlers": ["file"]
        }
    }
}

