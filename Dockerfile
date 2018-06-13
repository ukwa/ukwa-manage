FROM python:2.7-slim

# Additional dependencies required to support Snappy compression:
RUN apt-get update && apt-get install -y --no-install-recommends \
        libsnappy-dev \
        g++ \
		&& rm -rf /var/lib/apt/lists/*

# Install the package:
COPY . /ukwa-manage
RUN cd /ukwa-manage && pip install --no-cache-dir -r requirements.txt && python setup.py install

# Run LuigiD directly upon launch:
CMD ["luigid"]



