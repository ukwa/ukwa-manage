# This is just a dependency, for copying...
FROM ukwa/webarchive-discovery AS dep-env


# Switch to UKWA Hadoop 0.20 + Python 3 base image:
FROM ukwa/docker-hadoop:hadoop-0.20

# Switch to root user while installing software:
USER root

COPY ./cloudera-cdh3.list /etc/apt/sources.list.d/cloudera-cdh3.list

# Additional dependencies required to support Snappy compression:
RUN apt-get --allow-releaseinfo-change update && \
        apt-get update --allow-insecure-repositories  && \
        apt-get install -y --no-install-recommends --allow-unauthenticated \
        libsnappy-dev \
        g++ \
        git \
        zip \
        rustc \
        cargo \
        libssl-dev \
        python3-dev \
	&& rm -rf /var/lib/apt/lists/*

# Install the dependencies:
COPY requirements.txt /ukwa-manage/requirements.txt
RUN cd /ukwa-manage && \
    pip install setuptools && \
    pip install --no-cache-dir https://github.com/ukwa/hapy/archive/master.zip && \
    pip install --no-cache-dir https://github.com/ukwa/python-w3act/archive/master.zip && \
    pip install --no-cache-dir https://github.com/ukwa/crawl-streams/archive/master.zip && \
    pip install --no-cache-dir -r requirements.txt

# Install the package:
COPY setup.py /ukwa-manage/
COPY README.md /ukwa-manage/
COPY MANIFEST.in /ukwa-manage/
COPY lib /ukwa-manage/lib
RUN cd /ukwa-manage && python setup.py install

# Also copy in shell script helpers and configuration:
COPY scripts/* /usr/local/bin/
COPY mrjob.conf /etc/mrjob.conf

# Copy in the JARs from the dependent container:
COPY --from=dep-env /jars/* /usr/local/bin/

# Switch back to access user for running code:
USER access

