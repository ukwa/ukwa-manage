FROM centos:7

RUN \
  yum install -y epel-release && \
  yum install -y git python-pip python-devel && \
  pip install pika warctools requests

RUN \
  git clone https://github.com/ukwa/python-warcwriterpool.git && \
  cd python-warcwriterpool && python setup.py install

RUN \
  mkdir python-har-daemon

COPY harchiverd /python-har-daemon/harchiverd

WORKDIR python-har-daemon

VOLUME /logs

VOLUME /images

ENV LOG_FILE         "/logs/harchiverd.log"
ENV OUTPUT_DIRECTORY "/images"
ENV WEBSERVICE       "http://webrender:8000/webtools/domimage"
ENV AMQP_URL         "amqp://guest:guest@rabbitmq:5672/%2f"
ENV AMQP_EXCHANGE    "heritrix"
ENV AMQP_QUEUE       "to-webrender"
ENV AMQP_KEY         "to-webrender"

CMD python harchiverd/harchiverd.py
