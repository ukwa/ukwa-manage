FROM python:3.7-slim

# Additional dependencies required to support Snappy compression:
RUN apt-get update && apt-get install -y --no-install-recommends \
        libsnappy-dev \
        g++ \
        git \
	&& rm -rf /var/lib/apt/lists/*

# Install the dependencies:
COPY requirements.txt /ukwa-manage/requirements.txt
RUN cd /ukwa-manage && \
    pip install --no-cache-dir https://github.com/ukwa/hapy/archive/master.zip && \
    pip install --no-cache-dir https://github.com/ukwa/python-w3act/archive/master.zip && \
    pip install --no-cache-dir https://github.com/ukwa/crawl-streams/archive/master.zip && \
    pip install --no-cache-dir -r requirements.txt

# Install the package:
COPY setup.py /ukwa-manage/
COPY README.md /ukwa-manage/
COPY MANIFEST.in /ukwa-manage/
COPY lib /ukwa-manage/lib
COPY tasks /ukwa-manage/tasks
RUN cd /ukwa-manage && python setup.py install

# Also copy in shell script helpers:
COPY scripts/* /usr/local/bin/

