PID_FILE="/var/run/harchiverd/harchiverd.pid"
LOG_FILE="/var/log/harchiverd/harchiverd.log"
OUTPUT_DIRECTORY="/heritrix/output/images"
WEBSERVICE="http://webrender.bl.uk/webtools/domimage"
PROTOCOLS=["http", "https"]
AMQP_KEY="phantomjs"
AMQP_URL="amqp://guest:guest@192.168.45.26:5672/%2f"
AMQP_EXCHANGE="heritrix"
AMQP_OUTLINK_QUEUE="heritrix-outlinks"
AMQP_QUEUE="phantomjs"

