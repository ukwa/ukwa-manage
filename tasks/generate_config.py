#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
A simple utility to patch the available environment variables into the configuration used by Luigi.

Intended to be invoked with as:

    $ generate_config.py luigi.cfg.template luigi.cfg

"""

import ConfigParser, os, sys


def main():
    config = ConfigParser.ConfigParser(defaults=os.environ)
    config.read([sys.argv[1]])
    config.write(open(sys.argv[2], 'w'))

if __name__ == "__main__":
    main()