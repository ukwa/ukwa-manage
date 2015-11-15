import os
import logging

# Settings that can be overridden via environment variables.

LOG_FILE           = os.getenv("LOG_FILE", "/logs/harchiverd.log")
LOG_LEVEL          = os.getenv("LOG_LEVEL", logging.INFO)
OUTPUT_DIRECTORY   = os.getenv("OUTPUT_DIRECTORY", "/images")
WEBSERVICE         = os.getenv("WEBSERVICE", "http://webrender:8000/webtools/domimage")
PROTOCOLS          = ["http", "https"]
AMQP_URL           = os.getenv("AMQP_URL", "amqp://guest:guest@rabbitmq:5672/%2f")
AMQP_EXCHANGE      = os.getenv("AMQP_EXCHANGE", "heritrix")
AMQP_QUEUE         = os.getenv("AMQP_QUEUE", "to-webrender")
AMQP_KEY           = os.getenv("AMQP_KEY", "to-webrender")
AMQP_OUTLINK_QUEUE = os.getenv("AMQP_OUTLINK_QUEUE", "heritrix-outlinks")

#AMQP_URL="amqp://guest:guest@192.168.45.26:5672/%2f"

