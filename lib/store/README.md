store
=====

This is a wrapped for the WebHDFS API that provides some simple tooling suitable for our usage. In particular, it supports uploads verified by hash checks, and downloads of sections of WARC files so individual WARC records can be returned.

Run:

```
  store -h
```

to see the commands.

By default, it talks to the production HDFS API.


## Updating data from third-party sources

```
docker pull ukwa/ukwa-manage
source ~/gitlab/ukwa-services-env/nominet.env
docker run -ti -e NOM_HOST -e NOM_USER -e NOM_PWD ukwa/ukwa-manage store -u ingest nominet
```
