import os
import re
import xml.etree.ElementTree as ET

# Initial purpose is to scan a folder of ARKs and create the lookup table we need to resolve them.
# <ARK> <URL>
# OR
# <ARK> <TIMESTAMP> <URL>
#
# We will only extract the L-ARK for now, as this is used to resolve the resource.

# L-ARK: The Logical ARK for the Logical Document
# Usually ark:/81055/?vdc_############.0x000001

# MD-ARK: The Logical MD-ARK for the Document Metadata (not used by eJournals here)
# Usually ark:/81055/?vdc_############.0x000002

# D-ARK: The Digital-ARK (a.k.a. File ARK) for the Document data (bitstream)
# Usually ark:/81055/?vdc_############.0x000003

# METS_D_ARK: The D-ARK for the Mets Document
# Usually ark:/81055/?vdc_############.0x000004

l_ark_xpath = 'mets:structMap/mets:div[@CONTENTIDS]'
d_url_xpath = 'mets:fileSec/mets:fileGrp/mets:file/mets:FLocat[@xlink:href]'

ns = {
    "mets": "http://www.loc.gov/METS/",
    "xlink": "http://www.w3.org/1999/xlink"
}


def get_mapping(sip_file):
    dsip = ET.parse(open(sip_file))
    l_ark = dsip.find(l_ark_xpath, ns).get('CONTENTIDS')
    resolve_url = dsip.find(d_url_xpath, ns).get('{http://www.w3.org/1999/xlink}href')
    wb_suffix = resolve_url[len('https://www.webarchive.org.uk/access/resolve/'):]
    d_timestamp, d_url = wb_suffix[0:14], wb_suffix[15:]
    return l_ark, d_timestamp, d_url

sip_dir = '../../../test/data/document-sips/'
for filename in os.listdir(sip_dir):
    print("%s %s %s" % get_mapping(os.path.join(sip_dir,filename)))