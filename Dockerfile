# This is just a dependency, for copying...
FROM ukwa/webarchive-discovery AS dep-env


# Switch to UKWA Hadoop 0.20 + Python 3 base image:
FROM ukwa/docker-hadoop:2.1.2

# Switch to root user while installing software:
USER root

# Additional dependencies required to support Snappy compression:
RUN apt-get update && \
        apt-get install -y \
        libsnappy-dev \
        g++ \
        git \
        zip \
        rustc \
        cargo \
        libssl-dev \
        libffi-dev \
        python3-dev \
	&& rm -rf /var/lib/apt/lists/*

# Install the dependencies:
COPY requirements.txt /ukwa_manage/requirements.txt
RUN cd /ukwa_manage && \
    pip install -U setuptools pip wheel && \
    pip install --no-cache-dir https://github.com/ukwa/hapy/archive/master.zip && \
    pip install --no-cache-dir https://github.com/ukwa/python-w3act/archive/master.zip && \
    pip install --no-cache-dir https://github.com/ukwa/crawl-streams/archive/master.zip && \
    pip install --no-cache-dir -r requirements.txt

# Install the package:
RUN pip freeze
COPY setup.py /ukwa_manage/
COPY README.md /ukwa_manage/
COPY MANIFEST.in /ukwa_manage/
COPY lib /ukwa_manage/lib
RUN cd /ukwa_manage && pip install .

# Also copy in shell script helpers and configuration:
COPY scripts/* /usr/local/bin/
COPY mrjob.conf /etc/mrjob.conf
COPY mrjob_h3.conf /etc/mrjob_h3.conf

# Copy in the JARs from the dependent container:
COPY --from=dep-env /jars/* /usr/local/bin/

# Switch back to access user for running code:
USER access

