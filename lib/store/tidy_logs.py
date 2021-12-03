import os
import time
import argparse
import logging
from pathlib import Path
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

logging.basicConfig(level=logging.WARNING, format='%(asctime)s: %(levelname)s - %(name)s - %(message)s')

logger = logging.getLogger(__name__)


def tidy_up_logs(job_dir):
    # Get the job name:
    job_name = os.path.basename(job_dir)

    # These are the usual log files create by H3:
    log_filenames = [ 'alerts.log', 'crawl.log', 'nonfatal-errors.log', 'progress-statistics.log', 'runtime-errors.log', 'uri-errors.log' ]

    # List subfolders to find the most recent/active set of logs:
    subfolders = [ f.path for f in os.scandir(job_dir) if f.is_dir() ]
    last = sorted(subfolders)[-1]
    log_folder = f"{last}/logs"

    # Check the layout looks right
    if not os.path.exists(log_folder):
        raise Exception(f"Expected log folder {log_folder} does not exist!")
    if not os.path.isdir(log_folder):
        raise Exception(f"Expected log folder {log_folder} is not a folder!")

    # Set up metrics:
    registry = CollectorRegistry()
    g = Gauge('ukwa_crawler_log_size_bytes',
            'Total size of the most recent log files for the given job.',
            labelnames=['crawl_job_name', 'log'], registry=registry)
    gt = Gauge('ukwa_crawler_log_touch_seconds', 
            'The timestamp of the last time a log file had gone missing and was replaced with an empty file.',
            labelnames=['crawl_job_name', 'log'], registry=registry)

    for log_filename in log_filenames:
        log_path = f"{log_folder}/{log_filename}"
        # If there is a log file, check it's size and post that to Prometheus for monitoring:
        if os.path.exists(log_path):
            log_size = os.path.getsize(log_path)
            logger.info(f"Log {log_path} found, size = {log_size}")
            # Add result for this log
            g.labels(crawl_job_name=job_name, log=log_filename).set(log_size)

        # If there is no log file, wait a little:
        else:
            log_size = 0
            logger.warning(f"Log file {log_path} is missing.  Waiting 30s to see if it is mid-rotation....")
            # If there is still no log file after a little while, then create an empty one.
            time.sleep(30)
            if os.path.exists(log_path):
                logger.warning(f"The log file {log_path} has appeared - assuming log rotation is complete.")
            else:
                logger.warning(f"Creating an empty {log_path} file so crawler checkpointing will work.")
                Path(log_path).touch()
                gt.labels(crawl_job_name=job_name, log=log_filename).set_to_current_time()
                
    if os.environ.get("PUSH_GATEWAY"):
        push_to_gateway(os.environ.get("PUSH_GATEWAY"), job='tidy_logs', registry=registry)
    else:
        logger.error("No metrics PUSH_GATEWAY configured!")


def main():
    # Helper to tidy up WARC output folders:
    parser = argparse.ArgumentParser(prog='tidy-logs', description='Tidy up crawler logs and check on sizes.')
    parser.add_argument('--job-dir', 
        help="The job directory for the crawl to tidy. [default=%(default)s]", 
        default="/mnt/gluster/fc/heritrix/output/frequent-npld")

    args = parser.parse_args()

    # Run the tidy:
    tidy_up_logs(args.job_dir)    

if __name__ == "__main__":
    main()