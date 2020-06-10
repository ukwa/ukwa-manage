#!/bin/sh
docker pull ukwa/webarchive-discovery
docker pull ukwa/docker-hadoop:hadoop-0.20

docker build --tag ukwa-manage --build-arg http_proxy=${HTTP_PROXY} --build-arg https_proxy=${HTTPS_PROXY} .
