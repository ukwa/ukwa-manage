FROM python:2.7-slim

# Install the package:
COPY . /ukwa-manage
RUN cd /ukwa-manage && pip install --no-cache-dir -r requirements.txt && python setup.py install

# Run LuigiD directly upon launch:
CMD ["luigid"]



