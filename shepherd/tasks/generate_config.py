#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
A simple utility to patch the available environment variables into the configuration used by Luigi.

Intended to be invoked with as:

    $ generate_config.py luigi.cfg.template luigi.cfg

"""

import os, sys, string


def main():
    # Load the template file:
    template = open(sys.argv[1],'r').read()
    t = string.Template(template)

    # Map environment variables up to upper-case:
    env = dict((k.upper(), v) for k, v in os.environ.iteritems())

    # Perform the substitution:
    out = t.substitute(env)

    # Write out the result:
    with open(sys.argv[2], 'w') as f:
        f.write(out)

if __name__ == "__main__":
    main()
