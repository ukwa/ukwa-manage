python-w3act
==============

`python-w3act` is a Python package for handling interactions with the [w3act](https://github.com/ukwa/w3act/) service.

There are two main scripts: `w3add.py` and `w3start.py`.

#### w3add.py
This script is used to add new seeds to already-running crawls. It iterates over a series of exports (daily, weekly, etc. as exposed by w3act), comparing the start-date of each record to the current time. Those which should have started (i.e. their start-date is in the past) are then compared to the seeds in the running crawl. Seeds missing from the latter are then added.

An optional `-t` or `--timestamp` flag can be passed, in which case this will be used in place of the current time.

Optional `-f` or `--frequency` flag may be passed to restrict the range of exports which the script will review.

Passing the `-x` or `--test` flag will report on the number of seeds which _would_ have been added.

    usage: w3add.py [-h] [-t TIMESTAMP] [-f FREQUENCY [FREQUENCY ...]] [-x]

    Restarts Heritrix jobs.

    optional arguments:
      -h, --help            show this help message and exit
      -t TIMESTAMP, --timestamp TIMESTAMP
                            Timestamp
      -f FREQUENCY [FREQUENCY ...], --frequency FREQUENCY [FREQUENCY ...]
                            Frequency
      -x, --test            Test

#### w3start.py
This script is used to (re)start Heritrix jobs. Typically it is scheduled to run daily at noon. When run it will check whether the current time matches the criteria for each of the various frequencies. If so, the job is stopped (if already running) and a new one configured and started:

* Daily: every day at 12:00.
* Weekly: every Monday at 12:00.
* Monthly: the 1st of every month at 12:00.
* Quarterly: the 1st of January, April, July and October at 12:00.
* Six-monthly: the 1st of January, and July at 12:00.
* Annual: the 1st of January at 12:00.

An optional `-t` or `--timestamp` flag can be passed, in which case this will be used in place of the current time.

Optional `-f` or `--frequency` flag may be passed to restrict the range of exports which the script will review.

Passing the `-x` or `--test` will stop any running jobs and set up the new job but not start it.

      usage: w3start.py [-h] [-t TIMESTAMP] [-f FREQUENCY [FREQUENCY ...]] [-x]

    Restarts Heritrix jobs.

    optional arguments:
      -h, --help            show this help message and exit
      -t TIMESTAMP, --timestamp TIMESTAMP
                            Timestamp
      -f FREQUENCY [FREQUENCY ...], --frequency FREQUENCY [FREQUENCY ...]
                            Frequency
      -x, --test            Test

Setting the `SLACK` value to `True` in `settings` will enable log data to be sent to Slack. By default this is JSON-formatted but optionally the `SLACK_CSV` value may be set to `True` to additionally send a CSV file. Both of these rely on a properly configured value for `SLACK_TOKEN` which requires a correctly [authorised Slack app](https://api.slack.com/tokens). These will be sent to the channel identified by the `SLACK_CHANNEL` settings (note that this is the ID of the channel, not its name).