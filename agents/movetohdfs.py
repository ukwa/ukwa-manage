#!/usr/bin/env python
'''
Description:	Script to move files to HDFS
Author:		Gil
Date:		2016 April 26
'''

from __future__ import absolute_import

from crawl.hdfs import movetohdfs

# main --------------
if __name__ == "__main__":
    movetohdfs.main()
