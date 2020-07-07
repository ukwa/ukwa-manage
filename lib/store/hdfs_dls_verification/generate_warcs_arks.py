#!/usr/bin/env python

"""
Generic methods used for verifying/indexing SIPs.
"""

# libraries ---------------------------
from __future__ import absolute_import
import os
import sys
import argparse
import logging
import json
import hdfs
import tarfile
from io import BytesIO
from lxml import etree
from urllib.parse import urlparse

# globals -----------------------------
HDFS_URL='http://hdfs.api.wa.bl.uk'
HDFS_USER='access'
SIP_ROOT="/heritrix/sips"
NS={"mets": "http://www.loc.gov/METS/", "premis": "info:lc/xmlns/premis-v2"}
XLINK="{http://www.w3.org/1999/xlink}"
ID_PREFIX='hdfs://hdfs:54310'
DOWNLOAD_TEMPLATE=''


# functions ---------------------------
def setup_logging():
    global logger
    handler = logging.StreamHandler()
    formatter = logging.Formatter("[%(asctime)s] %(levelname)s %(filename)s.%(funcName)s: %(message)s")
    handler.setFormatter(formatter)
    logging.root.addHandler(handler)
    logging.root.setLevel(logging.WARN)
    logger = logging.getLogger(__name__)
    logger.setLevel( logging.INFO)

def script_die(msg):
    logger.error(msg)
    logger.error("Script died")
    sys.exit(1)

def get_args():
    parser = argparse.ArgumentParser('Extract ARKs and WARC details from HDFS')
    parser.add_argument('-o --outputfile', dest='outputFile', default='npld_warcs_arks.txt', help='Output file')
    global args
    args = parser.parse_args()
    if os.path.isfile(args.outputFile):
        script_die("Output file of [{}] already exists, exiting to not overwrite".format( args.outputFile))


# extract key values from xml
def process_xml(targz, f, xml):
    logger.info("Examining tar.gz {}\txml {}".format(targz, f))

    try:
        tree = etree.fromstring(xml)
        files = {}

        # process <mets:file entity, test values
        #### <mets:file MIMETYPE="application/warc" ADMID="AMDWARC0003" CHECKSUMTYPE="SHA-512" CHECKSUM="...." ID="WARC0003" SIZE="208574816">
        #### 	<mets:FLocat xmlns:xlink="http://www.w3.org/1999/xlink" LOCTYPE="URL" xlink:href="...."/>
        ####	<mets:transformFile TRANSFORMALGORITHM="WARC" TRANSFORMORDER="1" TRANSFORMTYPE="decompression"/>
        #### </mets:file>
        n_files = 0
        for metsfile in tree.xpath("//mets:file", namespaces=NS):
            adm_id = metsfile.attrib["ADMID"]
            if adm_id:
                href = metsfile.xpath("mets:FLocat", namespaces=NS)[0].attrib["{}href".format(XLINK)]
                if not href:
                    logger.error("HREF not extracted from {} {}".format(targz, f))
                hdfs_path = urlparse(href).path.replace('/webhdfs/v1', '', 1)
                # Checksum:
                checksum_type = metsfile.attrib["CHECKSUMTYPE"]
                if not checksum_type:
                    logger.error("Checksum_type not extract from {} {}".format(targz, f))
                checksum = metsfile.attrib["CHECKSUM"]
                try:
                    h = int(checksum, 16)
                except:
                    logger.error("Checksum not hex [{}] from {} {}".format(checksum, targz, f))
                if checksum_type == 'SHA-512':
                    hash_urn = 'urn:hash::sha512:%s' % checksum
                else:
                    raise Exception("Unsupported checksum type %s!" %checksum_type )
                # Size:                    
                size = int(metsfile.attrib["SIZE"])
                try:
                    if not size > 0:
                        logger.error("Weird size [{}] from {} {}".format(size, targz, f))
                except:
                    logger.error("Size [{}] not numeric from {} {}".format(size, targz, f))
                mimetype = metsfile.attrib["MIMETYPE"]
                if not mimetype:
                    logger.error("Mimetype not extracted from {} {}".format(targz, f))

                files[adm_id] = {
                    "id" : 'hdfs://hdfs:54310%s' % hdfs_path,
                    #"sip_href_s" : href, # Not sure this adds much useful info.
                    "sip_mimetype_s" : mimetype,
                    "sip_size_l" : size,
                    "sip_hash_urn_s": hash_urn
                    }
                n_files = n_files + 1

            else:
                logger.error("ADMID not extracted from {} {}".format(targz, f))

        if n_files == 0:
            logger.error("No ADMID values in {} {}".format(targz, f))

        if len(files.keys()) != n_files:
            logger.error("More files than IDs for ADM_ID {} in {} {} - keys {}, files {}".format(adm_id, targz, f, len(files.keys()), n_files))

        # process <mets:amdSec, test values
        #### <mets:amdSec ID="AMDWARC0003">
        #### 	<mets:digiprovMD ID="DMDWARC0003">
        #### 		<mets:mdWrap MDTYPE="PREMIS:OBJECT">
        #### 			<mets:xmlData>
        #### 				<premis:object xmlns:premis="info:lc/xmlns/premis-v2" xsi:type="premis:file">
        #### 					<premis:objectIdentifier>
        #### 						<premis:objectIdentifierType>ARK</premis:objectIdentifierType>
        #### 						<premis:objectIdentifierValue>ark:/81055/vdc_100022564746.0x000003</premis:objectIdentifierValue>
        #### etc.
        n_amdsecs = 0
        for metsamdsec in tree.xpath("//mets:amdSec", namespaces=NS):
            adm_id = metsamdsec.attrib["ID"]
            oit = metsamdsec.xpath("mets:digiprovMD/mets:mdWrap/mets:xmlData/premis:object/premis:objectIdentifier/premis:objectIdentifierType", namespaces=NS)
            if oit:
                if len(oit) == 1:
                    oit_text = oit[0].text
                    if oit_text == 'ARK':
#                        files[adm_id]['oit'] = oit_text

                        oiv = metsamdsec.xpath("mets:digiprovMD/mets:mdWrap/mets:xmlData/premis:object/premis:objectIdentifier/premis:objectIdentifierValue", namespaces=NS)
                        if oiv:
                            if len(oiv) == 1:
                                files[adm_id]['sip_ark_s'] = oiv[0].text
                                n_amdsecs = n_amdsecs + 1
                                yield files[adm_id]
                            else:
                                logger.error("More than one ObjIDValue found for {} {}".format(targz, f))
                        else:
                            logger.error("No ObjIDValue found for {} {}".format(targz, f))
                    else:
                        logger.error("ObjIDType not ARK [{}] for {} {}".format(targz, f, oit_text))
                else:
                    logger.error("More than one ObjIDType found for {} {}".format(targz, f))
            else:
                logger.debug("No ObjIDType found for {} in {} {}".format(adm_id, targz, f))

        if len(files.keys()) != n_amdsecs:
            logger.error("Failed to find same number of amdSec elements for ID {} in {} {} - keys {}, amdsecs {}".format(adm_id, targz, f, len(files.keys()), n_amdsecs))

    except IndexError as ie:
        logger.error("Problem parsing XML: {} {}".format(targz, f))


# find xml file inside sip tar.gz and pass on for processing
def find_sip_xml(hdfsClient, outputFile):
    with open(outputFile, 'w') as out:
        # for each file under SIP_ROOT
        for (path, dirs, files) in hdfsClient.walk(SIP_ROOT):
            for file in files:
                # filter for only tar.gz files that are not empty
                if file.endswith('.tar.gz'):
                    targz = "{}/{}".format(path, file)
                    targzStatus = hdfsClient.status(targz, strict=False)
                    if targzStatus['length'] > 0:
                        sip_path = "%s/%s" % (path, file)
                        sip = sip_path[len(SIP_ROOT) + 1:]
                        sip = sip[:-7]

                        # untar file, traverse internal files, extract contents of .xml file for processing
                        with hdfsClient.read(targz) as reader:
                            t = reader.read()
                            tar = tarfile.open(mode="r:gz", fileobj=BytesIO(t))

                            foundMets = 0
                            for f in tar.getmembers():
                                if f.name.endswith(".xml"):
                                    foundMets += 1
                                    xml = tar.extractfile(f).read()
                                    for waid in process_xml(targz, f.name, xml):
                                        waid['sip_hfds_path_s'] = sip_path
                                        waid['sip_id_s'] = sip
                                        out.write(json.dumps(waid))
                                        out.write('\n')

                            if foundMets == 0:
                                logger.error("No METS XML file found in {}".format(targz))
                            elif foundMets > 1:
                                logger.error("More than one XML file found in {}".format(targz))
                    else:
                        logger.warning("Empty (zero byte) SIP package: {}".format(targz))

def main():
    setup_logging()
    get_args()
    hdfsClient = hdfs.InsecureClient(HDFS_URL, HDFS_USER)
    find_sip_xml(hdfsClient, args.outputFile)

if __name__ == "__main__":
    main()
