#!/usr/bin/env python
'''
Description:	Control overall crawl jobs and workflows
Author:		Andy Jackson
Date:		2016-07-26
'''

import sys
import argparse
import luigi
import tasks.crawl_job_tasks


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

    # Resume
    start = subparsers.add_parser('resume', help="Start a job, resuming progress from the last checkpoint.")
    start.add_argument(**job_id_args)

    # Validate Job
    vj = subparsers.add_parser('validate_job', help="Validate the output of a job and bundle associated log files etc.")
    vj.add_argument(**job_id_args)
    vj.add_argument(**launch_id_args)

    # Build SIP
    bs = subparsers.add_parser('build_sip', help="Build a SIP from the data and log files for a given job launch.")
    bs.add_argument(**job_id_args)
    bs.add_argument(**launch_id_args)

    # Create Domain Crawl Job Definitions
    dc = subparsers.add_parser('create_dc_jobs', help="Build a set of job suitable for a domain crawl.")
    dc.add_argument(dest="dc_num_crawlers", help="Number of crawl jobs to use.")
    dc.add_argument(dest='dc_crawler', help="Host that will run the crawl.")

    # Print help if no arg specified:
    if len(sys.argv) < 2:
        sys.argv.append('--help')

    parsed = parser.parse_args()

    # Got a command, so do the action:

    if parsed.command == "stop":
        print("Stopping and checkpointing job %s" % parsed.job_id)
        luigi.run(['pulse.StopJob', '--job', parsed.job_id])

    elif parsed.command == "start":
        print("(Re)starting job %s" % parsed.job_id)
        luigi.run(['pulse.StartJob', '--job', parsed.job_id])

    elif parsed.command == "resume":
        print("Resuming job %s from latest checkpoint" % parsed.job_id)
        luigi.run(['pulse.StartJob', '--job', parsed.job_id, '--from-latest-checkpoint'])

    elif parsed.command == "build_sip":
        print("Building SIP for %s/%s" % (parsed.job_id, parsed.launch_id))
        luigi.run(['package.BuildSip', '--job', parsed.job_id, '--launch', parsed.launch_id])

    elif parsed.command == "create_dc_jobs":
        print("Creating Domain Crawl job definitions for %i jobs." % parsed.dc_num_crawlers)
        luigi.run(['dc.CreateDomainCrawlJobs' ,'--num-jobs', parsed.dc_num_crawlers, '--host', parsed.dc_crawler])



if __name__ == "__main__":
    main()

