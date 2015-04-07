#!/usr/bin/env python

import os
import sys
import w3act
import webhdfs
import requests
import subprocess
from lxml import etree
from urlparse import urlparse
from collections import Counter
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta

LDREPORT = "/heritrix/git/heritrix_bl_configs/bin/ld-report"

frequencies = ["daily", "weekly", "monthly", "quarterly", "sixmonthly", "annual", "domaincrawl", "nevercrawl"]

ldls = [("The British Library", "DLS-LON-WB01"), ("Trinity College Dublin", "DLS-BSP-WB04"), ("Bodleian Library", "DLS-BSP-WB03"), ("Cambridge University Library", "DLS-BSP-WB02")]

a = w3act.ACT()
w = webhdfs.API(prefix="http://dls.httpfs.wa.bl.uk:14000/webhdfs/v1")

orgs = []
schedules = []
wct_uids = 0
ends_with_uk = 0
non_ends_with_uk = 0
ends_with_london = 0
field_uk_domain = 0
field_uk_geoip = 0
field_uk_postal_address = 0
field_via_correspondence = 0
field_professional_judgement = 0

for frequency in frequencies:
    ex = a.get_ld_export(frequency)
    for node in ex:
#        orgs.append( node.find( "organisation" ).text )
        schedules.append(frequency)
        for url in [u["url"] for u in node["fieldUrls"]]:
            if urlparse( url ).netloc.endswith( ".uk" ):
                ends_with_uk += 1
                field_uk_domain += 1
            elif urlparse( url ).netloc.endswith( ".london" ):
                ends_with_london += 1
                field_uk_domain += 1
            elif urlparse( url ).netloc.endswith( ".wales" ):
                ends_with_wales += 1
                field_uk_domain += 1
            elif urlparse( url ).netloc.endswith( ".cymru" ):
                ends_with_cymru += 1
                field_uk_domain += 1
            elif urlparse( url ).netloc.endswith( ".scot" ):
                ends_with_scot += 1
                field_uk_domain += 1
            else:
                non_ends_with_uk += 1
        if node[ "field_uk_hosting" ]:
            field_uk_geoip += 1
        if node[ "field_uk_postal_address" ]:
            field_uk_postal_address += 1
        if node[ "field_via_correspondence" ]:
            field_via_correspondence += 1
        if node[ "field_professional_judgement" ]:
            field_professional_judgement += 1

# Check the UKWA-licensed content:
by = a.get_by_export(all)
wct_uids = len(by)

new_instances = 0
targets = w.list( "/data/wayback/cdx-index/" )["FileStatuses"]["FileStatus"]
for target in targets:
    instances = w.list( "/data/wayback/cdx-index/%s/" % target["pathSuffix"] )["FileStatuses"]["FileStatus"]
    for instance in instances:
        mod = datetime.fromtimestamp(instance["modificationTime"]/1000)
        if mod < (datetime.now() - relativedelta(months=-1)):
            new_instances += 1

new_sips = 0
dirs = w.list( "/heritrix/sips/" )["FileStatuses"]["FileStatus"]
for dir in dirs:
    sips = w.list( "/heritrix/sips/%s/" % dir["pathSuffix"] )["FileStatuses"]["FileStatus"]
    for sip in sips:
        t = datetime.fromtimestamp(sip["modificationTime"]/1000)
        if t > ( datetime.now() + relativedelta( months=-1 ) ):
            new_sips+=1

def print_dict( o, dict ):
    for key in sorted( dict, key=dict.get, reverse=True ):
        o.write( "<tr><td></td><td>%s</td><td style=\"align: right\">%s</td></tr>\n" % ( str( key ), str( dict[ key ] ) ) )

orgs = Counter( orgs )
schedules = Counter( schedules )
with open( "/var/www/html/act/monthly-stats.html", "wb" ) as o:
    o.write( "<!DOCTYPE html>\n<html>\n<head />\n<body>\n" )
    o.write( "<h2>%s</h2>\n" % datetime.now().strftime( "%Y-%m-%d" ) )
    o.write( "<table>\n" )
    o.write( "<tr><td>Total no of ACT records:</td></tr>\n" )
    print_dict( o, orgs )
    o.write( "<tr><td>No of ACT records with UKWA Selective Archive Licence:</td><td style=\"align: right\">%s</td></tr>\n" % wct_uids )
    o.write( "<tr><td>Total number of .uk URLs in ACT</td><td style=\"align: right\">%s</td></tr>\n" % ends_with_uk )
    o.write( "<tr><td>Total number of .london URLs in ACT:</td><td style=\"align: right\">%s</td></tr>\n" % ends_with_london )
    o.write( "<tr><td>Total number of .wales URLs in ACT:</td><td style=\"align: right\">%s</td></tr>\n" % ends_with_wales )
    o.write( "<tr><td>Total number of .cymru URLs in ACT:</td><td style=\"align: right\">%s</td></tr>\n" % ends_with_cymru)
    o.write( "<tr><td>Total number of .scot URLs in ACT:</td><td style=\"align: right\">%s</td></tr>\n" % ends_with_scot)
    o.write( "<tr><td>Total number of non UK Domain URLs in ACT:</td><td style=\"align: right\">%s</td></tr>\n" % non_ends_with_uk )
    o.write( "<tr><td>Summary crawl schedules report:</td></tr>\n" )
    print_dict( o, schedules )
    o.write( "<tr><td>UK Domain</td><td style=\"align: right\">%s</td></tr>\n" % field_uk_domain )
    o.write( "<tr><td>UK GeoIP:</td><td style=\"align: right\">%s</td></tr>\n" % field_uk_geoip )
    o.write( "<tr><td>UK Postal Address:</td><td style=\"align: right\">%s</td></tr>\n" % field_uk_postal_address )
    o.write( "<tr><td>Via Correspondence:</td><td style=\"align: right\">%s</td></tr>\n" % field_via_correspondence )
    o.write( "<tr><td>Professional Judgement:</td><td style=\"align: right\">%s</td></tr>\n" % field_professional_judgement )
    o.write( "<tr><td>SIPs Created:</td><td style=\"align: right\">%s</td></tr>\n" % new_sips )
    o.write( "<tr><td>Instances migrated to UKWA:</td><td style=\"align: right\">%s</td></tr>\n" % new_instances )
    today = date.today()
    last_month = today - relativedelta(months=1)
    for name, server in ldls:
        subprocess.Popen([LDREPORT, last_month.strftime("%b"), last_month.strftime("%Y"), server, name], stdout=o, stderr=subprocess.STDOUT)

with open( "/var/www/html/act/monthly-stats.html", "ab" ) as o:
    o.write( "</table>\n</body>\n</html>" )

with open( "/var/www/html/act/monthly-stats.html", "rb" ) as i:
    print i.read()

