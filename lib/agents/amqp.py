import abc
import pika
import time
import logging


# Set up a logging handler:
handler = logging.StreamHandler()
#handler = logging.StreamHandler(sys.stdout) # To use stdout rather than the default stderr
formatter = logging.Formatter( "[%(asctime)s] %(levelname)s %(filename)s.%(funcName)s: %(message)s" )
handler.setFormatter( formatter ) 

# Attach to root logger
logging.root.addHandler( handler )

# Set default logging output for all modules
logging.root.setLevel( logging.WARNING )

# Set up a logger for this class
logger = logging.getLogger( "amqp" )

class QueueConsumer(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, ampq_url, queue_name, exchange, routing_key):
        self.ampq_url = ampq_url
        self.queue_name = queue_name
        self.exchange = exchange
        self.routing_key = routing_key
        self.DUMMY = False

    @abc.abstractmethod
    def callback( self, ch, method, properties, body ):
        """This method should consume the message and do whatever else need to be done."""
        return

    def begin(self):
        try:
            if self.DUMMY:
                logger.warning( "Running in dummy mode." )
            logger.info( "Starting connection %s:%s." % ( self.ampq_url, self.queue_name ) )
            parameters = pika.URLParameters(self.ampq_url)
            connection = pika.BlockingConnection( parameters )
            channel = connection.channel()
            channel.exchange_declare(exchange=self.exchange, durable=True)
            channel.queue_declare( queue=self.queue_name, durable=True )
            channel.queue_bind(queue=self.queue_name, exchange=self.exchange, routing_key=self.routing_key)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume( self.callback, queue=self.queue_name )
            channel.start_consuming()
        except Exception as e:
            logger.error( str( e ) )
            logging.exception(e)
            logger.info("Sleeping for 10 seconds before a restart is attempted...")
            time.sleep(10)
