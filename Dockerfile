FROM centos:7

RUN \
  yum install -y epel-release && \
  yum install -y git python-pip python-devel && \
  pip install pika warctools requests

RUN \
  git clone https://github.com/ukwa/python-warcwriterpool.git && \
  cd python-warcwriterpool && python setup.py install

RUN \
  git clone https://github.com/ukwa/python-har-daemon.git && echo done

WORKDIR python-har-daemon

COPY settings.py /python-har-daemon/harchiverd/settings.py

VOLUME /logs

VOLUME /images

CMD python harchiverd/harchiverd.py
