w3act
==============
`w3act` is a Python package for interraction with the [w3act](https://github.com/ukwa/w3act/) service.

##### `job`
The primary class herein is the `W3actJob`, a representation of a `w3act` job. An instance can be built from an already-existing directory (see `w3start.py` for an example):

    from w3act.job import W3actJob
    existing_job = W3actJob.from_directory("/path/to/heritrix/job/")

Similarly, a new job can be created. This requires, however, an existing `heritrix` instance:

    import heritrix
    from w3act.job import W3actJob
    heritrix_api = heritrix.API(host="...", ...)
    new_job = W3actJob([...], name="daily", heritrix=heritrix_api)

A `W3actJob` instance can then be manipulated via its `start()`, `stop()` and `restart()` methods.

Note that this library will store basic information about the `w3act` Targets added to the crawl in a JSON-formatted file, `w3act-info.json`, in the job directory.

##### `settings`
Hopefully this is self-explanatory; it contains package-specific settings including server names and directory locations.

##### `w3actd`
In the package's `bin` directory there is an `init.d` script controlled via the typical `stop`, `start`, `restart` commands. This package contains the `JobDaemon` which does the bulk of its work.

The daemon monitors a queue (defined in the `settings` package above) for messages from `w3act` and should any arrive, starts a new job.

