FROM centos:7

RUN \
  yum install -y epel-release && \
  yum install -y git python-pip python-devel libpng-devel libjpeg-devel gcc gcc-c++ make libffi-devel openssl-devel supervisor libxml2 libxml2-devel libxslt libxslt-devel

RUN \
  pip install requests[security] pika gunicorn supervisor-stdout warctools python-dateutil lxml pywebhdfs hapy-heritrix bagit

COPY requirements.txt /

RUN pip install --no-cache-dir -r requirements.txt

RUN pip install -e git+https://github.com/ukwa/python-warcwriterpool.git@eceef73#egg=python_warcwriterpool

COPY crawl /shepherd/crawl
COPY setup.py /shepherd/setup.py
COPY README.md /shepherd/README.md
COPY requirements.txt /shepherd/requirements.txt

RUN cd /shepherd/ && python setup.py install

# This is needed to force SupervisorD to run as root.
# TODO Avoid this in future, as it should not be necessary even under Docker.
ENV C_FORCE_ROOT TRUE

ADD supervisord.conf /etc/supervisor/conf.d/supervisord.conf

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]



