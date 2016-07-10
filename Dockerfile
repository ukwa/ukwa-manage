FROM python:2.7

COPY requirements.txt /

RUN pip install --no-cache-dir -r requirements.txt

RUN pip install -e git+https://github.com/ukwa/python-warcwriterpool.git@eceef73#egg=python_warcwriterpool

COPY crawl /crawl
COPY lib /lib

ENV C_FORCE_ROOT TRUE

CMD celery -A crawl worker --autoreload  --loglevel=info



