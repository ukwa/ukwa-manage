import os
from urlparse import urlparse


def url_to_surt(in_url, host_only=False):
    '''
    Converts a URL to SURT form.
    '''
    parsed = urlparse(in_url)
    authority = parsed.netloc.split(".")
    authority.reverse()
    surt = "http://(%s," % ",".join(authority)
    if parsed.path and not host_only:
        surt = "%s%s" %( surt , os.path.dirname(parsed.path) )
    return surt
