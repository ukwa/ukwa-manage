windex
=======

The UKWA index management tool.

Start at [cmd.py](./cmd.py) to look at how it works.

n.b. by default, this tool will talk to:

- the development TrackDB (`trackdb.dapi.wa.bl.uk`)
- the development CDX service (`cdx.dapi.wa.bl.uk`) 
- the development Solr services (`dev-zk1:2182,dev-zk2:2182,dev-zk3:2182`)
- the production store (`hdfs.api.wa.bl.uk`)

These can be overidden via environment variables or command-line arguments.


## Running Indexing Tasks

Note that the following examples run the windex tool directly.  In production, the tools would usually be run inside versioned Docker containers, so the invocation is slightly more complicated. These scripts should usually be in the [`ukwa-services` repository](https://github.com/ukwa/ukwa-services).


### CDX Indexing:

For CDX indexing, we have chosed to add a multivalued status field to record what we're doing, and called it `cdx_index_ss`. 

1. The WARCs on the storage service start off having no value of this field, and this fact is used to identify those that require indexing. 
2. After indexing, we assign two values to the `cdx_index_ss` field: `<COLLECTION>` and `<COLLECTION>_unverified` to indicate that we've indexed the WARC into a CDX collection called `COLLECTION`, but not checked the index worked (yet). 
3. Once the indexing has been verified, the `<COLLECTION>_unverified` flag can be removed.


The `windex` tool is used to manage this process. For example,

```
windex cdx-index \
  --trackdb-url "http://trackdb.api.wa.bl.uk/solr/tracking" \
  --stream frequent \
  --year 2020 \
  --cdx-collection data-heritrix \
  --cdx-service "http://cdx.api.wa.bl.uk" \
  --batch-size 1000
```

This will talk to the production TrackDB, and get a list of the 1000 most recent WARCs from the 2020 frequent crawls that are not yet marked as contained in the `data-heritrix` CDX collection.  It then runs the Hadoopm indexing job for those WARCS, checks the output, and if all looks well, updates the TrackDB as outlined in step 2 above.

Note that the `trackdb` command can be used to query the TrackDB and check what's going on. To get a list of records for WARCs that have not yet been indexed:

    trackdb warcs --field cdx_index_ss _NONE_ list > warcs-to-index.jsonl

This lists return the 100 most recent matching files by default, and can be filtered and limited in various ways (see `trackdb -h` for details). The command returns detailed information in JSONL format by default.


### CDX Verification

_The `cdx-verify` step has not yet been moved over to this new approach._

### Solr Indexing

This works in the same way as the CDX indexing, using a tracking field called `solr_index_ss` and using the SolrCloud collection name as the value. e.g. this runs a test job against the development TrackDB and SolrCloud:

```
  windex cdx-index \
    --trackdb-url "http://trackdb.dapi.wa.bl.uk/solr/tracking" \
    --stream frequent \
    --year 2020 \
    --solr-collection fc-2020-test \
    --zks "dev-zk1:2182,dev-zk2:2182,dev-zk3:2182" \
    --batch-size 1000 \
    warc-npld.conf \
    annotations.json \
    allows.txt
```

The main difference with the CDX indexing case is that we require three additional configuration files; `warc-npld.conf`, which is the general indexer configuration file that controls which features to extract; `annotation.json`, which contains the list of additional annotations to add, e.g. which collections and subjects a URL belongs to; and `allows.txt` which provides the SURT prefixes that are considered open access.

These last two configuration files should be regularly generated from W3ACT, using the [`python-w3act`](https://github.com/ukwa/python-w3act) `w3act` command.

## Queries

We can query the CDX from the command-line:

```
$ windex -C ethos cdx-query http://theses.gla.ac.uk/1158/1/2009ibrahamphd.pdf
uk,ac,gla,theses)/1158/1/2009ibrahamphd.pdf 20200404014648 http://theses.gla.ac.uk/1158/1/2009ibrahamphd.pdf application/pdf 200 FH7MXPURQT7S75IVEUUFWPA2XPOTY3VW - - 7803924 643334769 /1_data/ethos/warcs/WARCPROX-20200404014942362-00230-mja43xl7.warc.gz
```

Now we use the filename, offset and length to grab the WARC record:

```
$ store get --offset 643334769 --length 7803924 /1_data/ethos/warcs/WARCPROX-20200404014942362-00230-mja43xl7.warc.gz temp.warc.gz
```

This gets the WARC record (and oddly, all following ones?!)

```
$ warcio extract --payload temp.warc.gz 0 > file.pdf
```

So now we have the PDF.
