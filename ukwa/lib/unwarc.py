import os
import requests
from warcio.archiveiterator import ArchiveIterator
import mimetypes
from urlparse import urlparse


def print_records(url):
    resp = requests.get(url, stream=True)

    for record in ArchiveIterator(resp.raw, arc2warc=True):
        if record.rec_type == 'warcinfo':
            print(record.raw_stream.read())

        elif record.rec_type == 'response':
            if record.http_headers:
                content_type = record.http_headers.get_header('Content-Type')
                uri = record.rec_headers.get_header('WARC-Target-URI')
                parsed = urlparse(uri)
                path = os.path.join("./", parsed.netloc, parsed.path.lstrip('/'))
                # Check for an extension and attempt to add one if there is not one there already:
                name, ext = os.path.splitext(parsed.path)
                if not ext:
                    extension = mimetypes.guess_extension(content_type)
                    if extension and not path.endswith(extension):
                        print("Adding "+extension)
                        path = "%s%s" % (path, extension)
                print(path)
                #print(record.content_stream().read())
            else:
                print('skipping %s' % record.rec_headers.get_header('WARC-Target-URI'))
        else:
            pass
            #print('skipping %s %s' % ( record.rec_type, record.rec_headers.get_header('WARC-Target-URI')))


# WARC
print_records('https://archive.org/download/ExampleArcAndWarcFiles/IAH-20080430204825-00000-blackbook.warc.gz')




