FROM python:2.7-slim

# Additional dependencies required to support Snappy compression:
RUN apt-get update && apt-get install -y --no-install-recommends \
        libsnappy-dev \
        g++ \
        git \
	&& rm -rf /var/lib/apt/lists/*

# Install the package:
COPY . /ukwa-manage
RUN cd /ukwa-manage && pip install --no-cache-dir https://github.com/ukwa/hapy/archive/master.zip && pip install --no-cache-dir -r requirements.txt && python setup.py install

# Run the dashboard:
CMD gunicorn --error-logfile - --access-logfile - --bind 0.0.0.0:8000 --workers 10 dash.dashboard:app



