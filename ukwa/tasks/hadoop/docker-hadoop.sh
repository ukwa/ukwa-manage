#!/bin/sh
/usr/local/bin/docker-compose -f ../../docker-compose.yml exec hadoop /usr/local/hadoop/bin/hadoop $*
