import pika

QUEUE_HOST = "192.168.45.26"
ERROR_QUEUE = "sip-error"
SUBMISSION_QUEUE = "sips"


def resend_one():
    connection = pika.BlockingConnection(pika.ConnectionParameters(QUEUE_HOST))
    channel = connection.channel()
    method_frame, header_frame, body = channel.basic_get(ERROR_QUEUE)
    if method_frame:
        print method_frame, header_frame, body
        tag = body.split('|')[0]
        print tag
        channel.confirm_delivery()
        channel.basic_publish(exchange="",
                              routing_key=SUBMISSION_QUEUE,
                              properties=pika.BasicProperties(
                                  delivery_mode=2,
                              ),
                              body=tag
                              )
        # (Re)publish confirmed, so ACK:
        channel.basic_ack(method_frame.delivery_tag)
        connection.close()
    else:
        connection.close()
        raise 'No message returned'


# weekly/20170126115042

if __name__ == "__main__":
    resend_one()
