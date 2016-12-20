#!/bin/sh

# Generate the Luigi configuration:
generate-luigi-config /etc/luigi/luigi.cfg.template /etc/luigi/luigi.cfg

# And launch the Luigi daemon:
luigid
