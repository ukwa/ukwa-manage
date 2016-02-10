'''
Created on 10 Feb 2016

@author: andy
'''

import os
from urlparse import urlparse

def url_to_surt(in_url):
	'''
	Converts a URL to SURT form.
	'''
	parsed = urlparse(in_url)
	authority = parsed.netloc.split(".")
	authority.reverse()
	surt = "http://(%s," % ",".join(authority)
	if parsed.path:
		surt = "%s%s" %( surt , os.path.dirname(parsed.path) )
	return surt
