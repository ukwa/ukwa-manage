FROM centos:7

# Set up basic Python 2.7 on Centos environment:
RUN \
  yum install -y epel-release && \
  yum install -y git python-pip python-devel libpng-devel libjpeg-devel gcc gcc-c++ make libffi-devel openssl-devel supervisor libxml2 libxml2-devel libxslt libxslt-devel

RUN yum install -y cronie

# Install the Shepherd package:
COPY . /shepherd
RUN cd /shepherd/ && pip install --no-cache-dir -r requirements.txt
RUN cd /shepherd/ && python setup.py install

# Set up configuration for supervisor:
ADD supervisord.conf.docker /etc/supervisor/conf.d/supervisord.conf

# Add the crontab:
RUN crontab /shepherd/crontab.root.docker

# And add the luigi configuration:
ADD luigi.cfg.template /etc/luigi/luigi.cfg.template
ADD launch-luigi-docker.sh /

# This is needed to force SupervisorD to run as root.
# TODO Avoid this in future, as it should not be necessary even under Docker.
ENV C_FORCE_ROOT TRUE

# Run supervisord on launch:
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]



