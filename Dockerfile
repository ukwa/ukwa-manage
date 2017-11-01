FROM centos:7

# Set up basic Python 2.7 on Centos environment:
RUN \
  yum install -y epel-release && \
  yum install -y git python-pip python-devel libpng-devel libjpeg-devel gcc gcc-c++ make libffi-devel openssl-devel supervisor libxml2 libxml2-devel libxslt libxslt-devel

RUN yum install -y cronie

# Install the Shepherd package:
COPY . /ukwa-monitor
RUN cd /ukwa-monitor && pip install --no-cache-dir -r requirements.txt && python setup.py install

# Add the crontab:
RUN crontab /ukwa-monitor/crontab.root.docker

ENV PYTHONUNBUFFERED="TRUE"

# Run LuigiD directly upon launch:
CMD ["luigid"]



