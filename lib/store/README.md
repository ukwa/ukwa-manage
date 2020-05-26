store
=====

This is a wrapped for the WebHDFS API that provides some simple tooling suitable for our usage. In particular, it supports uploads verified by hash checks, and downloads of sections of WARC files so individual WARC records can be returned.

Run:

```
  store -h
```

to see the commands.

By default, it talks to the prodiction HDFS API.