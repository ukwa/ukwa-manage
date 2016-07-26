#!/usr/bin/env python
'''
Description:	Control overall crawl jobs and workflows
Author:		Andy Jackson
Date:		2016-07-26
'''

import sys
import argparse
import crawl.tasks

# main --------------
def main():
    parser = argparse.ArgumentParser('Control crawl jobs and workflows')

    # Reusable arguments
    job_id_args = {
        'dest' : "job_id",
        'help' : "Identifier of job to act on. One of 'daily', 'weekly', 'monthly, 'quarterly', 'sixmonthly', 'annual'."
    }

    launch_id_args = {
        'dest' : "launch_id",
        'help' : "Identifier of launch to process, e.g. '20160726083240'."
    }

    # add commands:
    subparsers = parser.add_subparsers(dest="command")

    # Stop
    stop = subparsers.add_parser('stop', help="Stop a currently running job.")
    stop.add_argument(**job_id_args)

    # Start
    start = subparsers.add_parser('start', help="Start a job, or restart a currently running job.")
    start.add_argument(**job_id_args)

    # Validate Job
    vj = subparsers.add_parser('validate_job', help="Validate the output of a job and bundle associated log files etc.")
    vj.add_argument(**job_id_args)
    vj.add_argument(**launch_id_args)


    # Print help if no arg specified:
    if len(sys.argv) < 2:
        sys.argv.append('--help')

    parsed = parser.parse_args()

    # Got a command:
    if parsed.command == "stop":
        print("Stopping job %s" % parsed.job_id)
        crawl.tasks.stop_start_job.delay('daily', restart=False)

    elif parsed.command == "start":
        print("(Re)starting job %s" % parsed.job_id)
        crawl.tasks.stop_start_job.delay('daily', restart=True)

if __name__ == "__main__":
    main()

