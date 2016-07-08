#!/usr/bin/env python

"""
Daemon which watches a configured queue for messages and for each, creates
submits to Heritrix.
"""

import logging
import sys

from crawl import settings
from w3act.w3actd import JobDaemon

logger = logging.getLogger("w3actd")
handler = logging.FileHandler("%s/%s.log" % (settings.LOG_ROOT, __name__))
formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

#Try to set logging output for all modules.
logging.root.setLevel(logging.DEBUG)
logging.getLogger("").addHandler(handler)

if __name__ == "__main__":
    """Sets up the daemon."""
    daemon = JobDaemon("%s/w3actd.pid" % settings.PID_ROOT)
    logger.debug("Arguments: %s" % sys.argv)
    if len(sys.argv) == 2:
        if "start" == sys.argv[1]:
            logger.info("Starting sipd.")
            daemon.start()
        elif "stop" == sys.argv[1]:
            logger.info("Stopping sipd.")
            daemon.stop()
        elif "restart" == sys.argv[1]:
            logger.info("Restarting sipd.")
            daemon.restart()
        else:
            print "Unknown command"
            print "usage: %s start|stop|restart" % sys.argv[0]
            sys.exit(2)
        logger.debug("Exiting.")
        sys.exit(0)
    else:
        print "usage: %s start|stop|restart" % sys.argv[0]
        sys.exit(2)

