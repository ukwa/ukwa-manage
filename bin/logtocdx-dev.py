#!/bin/bash
export AMQP_URL="amqp://guest:guest@192.168.99.100:5672/%2f"
export QUEUE_NAME=crawl-log-feed
export LOG_FILE=logtocdx.log
export DUMMY_RUN=

./bin/logtocdx.py
